package security

import (
	"context"
	"fmt"
	"sync"
	"time"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	secretmanagerpb "cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/evdnx/goexchange/config"
	"github.com/evdnx/golog"
	metrics "github.com/evdnx/gotrademetrics"
)

// KeyRotationService manages automatic rotation of API keys
type KeyRotationService struct {
	configManager    *config.ConfigManager
	logger           *golog.Logger
	metrics          *metrics.Metrics
	projectID        string
	rotationInterval time.Duration
	exchangeAdapters map[string]ExchangeKeyAdapter
	stopChan         chan struct{}
	wg               sync.WaitGroup
	mu               sync.Mutex
	isRunning        bool
	lastRotation     map[string]time.Time
}

// ExchangeKeyAdapter is an interface for exchange-specific key rotation
type ExchangeKeyAdapter interface {
	GenerateNewKeys(ctx context.Context, exchangeConfig config.ExchangeConfig) (string, string, error)
	RevokeOldKeys(ctx context.Context, exchangeConfig config.ExchangeConfig) error
}

// KeyRotationConfig contains configuration for the key rotation service
type KeyRotationConfig struct {
	Enabled          bool          `mapstructure:"enabled"`
	RotationInterval time.Duration `mapstructure:"rotationInterval" validate:"required_if=Enabled true"`
	ProjectID        string        `mapstructure:"projectID" validate:"required_if=Enabled true"`
}

// NewKeyRotationService creates a new key rotation service
func NewKeyRotationService(configManager *config.ConfigManager, config KeyRotationConfig, logger *golog.Logger, metrics *metrics.Metrics) *KeyRotationService {
	return &KeyRotationService{
		configManager:    configManager,
		logger:           logger,
		metrics:          metrics,
		projectID:        config.ProjectID,
		rotationInterval: config.RotationInterval,
		exchangeAdapters: make(map[string]ExchangeKeyAdapter),
		stopChan:         make(chan struct{}),
		lastRotation:     make(map[string]time.Time),
	}
}

// RegisterExchangeAdapter registers an exchange adapter for key rotation
func (s *KeyRotationService) RegisterExchangeAdapter(exchangeName string, adapter ExchangeKeyAdapter) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.exchangeAdapters[exchangeName] = adapter
	s.logger.Info("Registered exchange adapter for key rotation", golog.String("exchange", exchangeName))
	if s.metrics != nil {
		s.metrics.Config.ComponentsRegistered.Inc()
	}
}

// Start starts the key rotation service
func (s *KeyRotationService) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.isRunning {
		s.mu.Unlock()
		return fmt.Errorf("key rotation service is already running")
	}
	s.isRunning = true
	s.mu.Unlock()

	s.logger.Info("Starting key rotation service", golog.String("interval", s.rotationInterval.String()))

	s.wg.Add(1)
	go s.rotationLoop(ctx)

	return nil
}

// Stop stops the key rotation service
func (s *KeyRotationService) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.isRunning {
		return
	}

	s.logger.Info("Stopping key rotation service")
	close(s.stopChan)
	s.wg.Wait()
	s.isRunning = false
}

// RotateKeys rotates API keys for all configured exchanges
func (s *KeyRotationService) RotateKeys(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	startTime := time.Now()
	s.logger.Info("Starting key rotation for all exchanges")

	cfg := s.configManager.GetConfig()
	if cfg == nil {
		s.logger.Error("Configuration is nil")
		if s.metrics != nil {
			s.metrics.RecordLogError("key_rotation", metrics.ReasonInternal)
		}
		return fmt.Errorf("configuration is nil")
	}

	for _, exchange := range cfg.Exchanges {
		// Skip exchanges that don't use secrets
		if !exchange.UseSecrets {
			s.logger.Info("Skipping key rotation for exchange (not using secrets)", golog.String("exchange", exchange.Name))
			continue
		}

		// Skip exchanges without a registered adapter
		adapter, ok := s.exchangeAdapters[exchange.Name]
		if !ok {
			s.logger.Warn("Skipping key rotation for exchange (no adapter registered)", golog.String("exchange", exchange.Name))
			continue
		}

		s.logger.Info("Rotating API keys for exchange", golog.String("exchange", exchange.Name))

		// Generate new keys
		newAPIKey, newAPISecret, err := adapter.GenerateNewKeys(ctx, exchange)
		if err != nil {
			s.logger.Error(
				"Failed to generate new API keys",
				golog.String("exchange", exchange.Name),
				golog.String("error", err.Error()),
			)
			if s.metrics != nil {
				s.metrics.RecordLogError("key_rotation", metrics.ReasonAPIError)
				s.metrics.Security.KeyGenerationsTotal.WithLabelValues(exchange.Name).Inc()
			}
			continue
		}

		// Update secrets in GCP Secret Manager
		if err := s.updateSecret(ctx, exchange.APIKeySecretPath, newAPIKey); err != nil {
			s.logger.Error(
				"Failed to update API key secret",
				golog.String("exchange", exchange.Name),
				golog.String("error", err.Error()),
			)
			if s.metrics != nil {
				s.metrics.RecordLogError("key_rotation", metrics.ReasonInternal)
			}
			continue
		}

		if err := s.updateSecret(ctx, exchange.APISecretSecretPath, newAPISecret); err != nil {
			s.logger.Error(
				"Failed to update API secret",
				golog.String("exchange", exchange.Name),
				golog.String("error", err.Error()),
			)
			if s.metrics != nil {
				s.metrics.RecordLogError("key_rotation", metrics.ReasonInternal)
			}
			continue
		}

		// Revoke old keys
		if err := adapter.RevokeOldKeys(ctx, exchange); err != nil {
			s.logger.Warn(
				"Failed to revoke old API keys",
				golog.String("exchange", exchange.Name),
				golog.String("error", err.Error()),
			)
			if s.metrics != nil {
				s.metrics.RecordLogError("key_rotation", metrics.ReasonAPIError)
				s.metrics.Security.KeyRevocationsTotal.WithLabelValues(exchange.Name).Inc()
			}
			// Continue anyway, as new keys are in use
		} else if s.metrics != nil {
			s.metrics.Security.KeyRevocationsTotal.WithLabelValues(exchange.Name).Inc()
		}

		// Update last rotation time
		s.lastRotation[exchange.Name] = time.Now()

		s.logger.Info("Successfully rotated API keys for exchange", golog.String("exchange", exchange.Name))
		if s.metrics != nil {
			s.metrics.Security.KeyGenerationsTotal.WithLabelValues(exchange.Name).Inc()
		}
	}

	// Reload configuration to use the new keys
	if err := s.configManager.ResolveSecrets(ctx, s.projectID); err != nil {
		s.logger.Error("Failed to reload configuration after key rotation", golog.String("error", err.Error()))
		if s.metrics != nil {
			s.metrics.RecordLogError("key_rotation", metrics.ReasonInternal)
		}
		return err
	}

	if s.metrics != nil {
		s.metrics.RecordFunctionDuration("key_rotation", time.Since(startTime).Seconds())
	}
	return nil
}

// rotationLoop runs the key rotation loop
func (s *KeyRotationService) rotationLoop(ctx context.Context) {
	defer s.wg.Done()

	ticker := time.NewTicker(s.rotationInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := s.RotateKeys(ctx); err != nil {
				s.logger.Error("Failed to rotate keys", golog.String("error", err.Error()))
				if s.metrics != nil {
					s.metrics.RecordFunctionError("key_rotation")
				}
			}
		case <-s.stopChan:
			return
		case <-ctx.Done():
			return
		}
	}
}

// updateSecret updates a secret in GCP Secret Manager
func (s *KeyRotationService) updateSecret(ctx context.Context, secretPath, secretValue string) error {
	startTime := time.Now()
	client, err := secretmanager.NewClient(ctx)
	if err != nil {
		s.logger.Error("Failed to create secretmanager client", golog.String("error", err.Error()))
		if s.metrics != nil {
			s.metrics.RecordLogError("key_rotation", metrics.ReasonInternal)
		}
		return fmt.Errorf("failed to create secretmanager client: %w", err)
	}
	defer client.Close()

	name := fmt.Sprintf("projects/%s/secrets/%s", s.projectID, secretPath)
	addReq := &secretmanagerpb.AddSecretVersionRequest{
		Parent: name,
		Payload: &secretmanagerpb.SecretPayload{
			Data: []byte(secretValue),
		},
	}

	_, err = client.AddSecretVersion(ctx, addReq)
	if err != nil {
		s.logger.Error(
			"Failed to add secret version",
			golog.String("error", err.Error()),
			golog.String("secret", secretPath),
		)
		if s.metrics != nil {
			s.metrics.RecordLogError("key_rotation", metrics.ReasonInternal)
		}
		return fmt.Errorf("failed to add secret version: %w", err)
	}

	if s.metrics != nil {
		s.metrics.RecordFunctionDuration("update_secret", time.Since(startTime).Seconds())
	}
	return nil
}

// GetLastRotationTime returns the last rotation time for an exchange
func (s *KeyRotationService) GetLastRotationTime(exchangeName string) (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	lastRotation, ok := s.lastRotation[exchangeName]
	return lastRotation, ok
}

// ForceRotation forces an immediate key rotation for a specific exchange
func (s *KeyRotationService) ForceRotation(ctx context.Context, exchangeName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	startTime := time.Now()
	cfg := s.configManager.GetConfig()
	if cfg == nil {
		s.logger.Error("Configuration is nil")
		if s.metrics != nil {
			s.metrics.RecordLogError("key_rotation", metrics.ReasonInternal)
		}
		return fmt.Errorf("configuration is nil")
	}

	var exchangeConfig config.ExchangeConfig
	found := false
	for _, exchange := range cfg.Exchanges {
		if exchange.Name == exchangeName {
			exchangeConfig = exchange
			found = true
			break
		}
	}

	if !found {
		s.logger.Error("Exchange not found in configuration", golog.String("exchange", exchangeName))
		if s.metrics != nil {
			s.metrics.RecordLogError("key_rotation", metrics.ReasonInternal)
		}
		return fmt.Errorf("exchange %s not found in configuration", exchangeName)
	}

	if !exchangeConfig.UseSecrets {
		s.logger.Error("Exchange not using secrets", golog.String("exchange", exchangeName))
		if s.metrics != nil {
			s.metrics.RecordLogError("key_rotation", metrics.ReasonInternal)
		}
		return fmt.Errorf("exchange %s is not using secrets", exchangeName)
	}

	adapter, ok := s.exchangeAdapters[exchangeName]
	if !ok {
		s.logger.Error("No adapter registered for exchange", golog.String("exchange", exchangeName))
		if s.metrics != nil {
			s.metrics.RecordLogError("key_rotation", metrics.ReasonInternal)
		}
		return fmt.Errorf("no adapter registered for exchange %s", exchangeName)
	}

	s.logger.Info("Forcing API key rotation for exchange", golog.String("exchange", exchangeName))

	newAPIKey, newAPISecret, err := adapter.GenerateNewKeys(ctx, exchangeConfig)
	if err != nil {
		s.logger.Error(
			"Failed to generate new API keys",
			golog.String("exchange", exchangeName),
			golog.String("error", err.Error()),
		)
		if s.metrics != nil {
			s.metrics.RecordLogError("key_rotation", metrics.ReasonAPIError)
			s.metrics.Security.KeyGenerationsTotal.WithLabelValues(exchangeName).Inc()
		}
		return err
	}

	if err := s.updateSecret(ctx, exchangeConfig.APIKeySecretPath, newAPIKey); err != nil {
		s.logger.Error(
			"Failed to update API key secret",
			golog.String("exchange", exchangeName),
			golog.String("error", err.Error()),
		)
		if s.metrics != nil {
			s.metrics.RecordLogError("key_rotation", metrics.ReasonInternal)
		}
		return err
	}

	if err := s.updateSecret(ctx, exchangeConfig.APISecretSecretPath, newAPISecret); err != nil {
		s.logger.Error(
			"Failed to update API secret",
			golog.String("exchange", exchangeName),
			golog.String("error", err.Error()),
		)
		if s.metrics != nil {
			s.metrics.RecordLogError("key_rotation", metrics.ReasonInternal)
		}
		return err
	}

	if err := adapter.RevokeOldKeys(ctx, exchangeConfig); err != nil {
		s.logger.Warn(
			"Failed to revoke old API keys",
			golog.String("exchange", exchangeName),
			golog.String("error", err.Error()),
		)
		if s.metrics != nil {
			s.metrics.RecordLogError("key_rotation", metrics.ReasonAPIError)
			s.metrics.Security.KeyRevocationsTotal.WithLabelValues(exchangeName).Inc()
		}
	} else if s.metrics != nil {
		s.metrics.Security.KeyRevocationsTotal.WithLabelValues(exchangeName).Inc()
	}

	s.lastRotation[exchangeName] = time.Now()

	s.logger.Info("Successfully rotated API keys for exchange", golog.String("exchange", exchangeName))
	if s.metrics != nil {
		s.metrics.Security.KeyGenerationsTotal.WithLabelValues(exchangeName).Inc()
		s.metrics.RecordFunctionDuration("force_rotation", time.Since(startTime).Seconds())
	}

	if err := s.configManager.ResolveSecrets(ctx, s.projectID); err != nil {
		s.logger.Error("Failed to reload configuration after key rotation", golog.String("error", err.Error()))
		if s.metrics != nil {
			s.metrics.RecordLogError("key_rotation", metrics.ReasonInternal)
		}
		return err
	}

	return nil
}
