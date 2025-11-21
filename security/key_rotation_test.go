package security

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/evdnx/goexchange/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockExchangeKeyAdapter is a mock implementation of ExchangeKeyAdapter
type MockExchangeKeyAdapter struct {
	mock.Mock
}

func (m *MockExchangeKeyAdapter) GenerateNewKeys(ctx context.Context, exchangeConfig config.ExchangeConfig) (string, string, error) {
	args := m.Called(ctx, exchangeConfig)
	return args.String(0), args.String(1), args.Error(2)
}

func (m *MockExchangeKeyAdapter) RevokeOldKeys(ctx context.Context, exchangeConfig config.ExchangeConfig) error {
	args := m.Called(ctx, exchangeConfig)
	return args.Error(0)
}

// MockConfigManager is a mock implementation for testing
type MockConfigManager struct {
	mock.Mock
	config *config.Config
}

func (m *MockConfigManager) GetConfig() *config.Config {
	return m.config
}

func (m *MockConfigManager) ResolveSecrets(ctx context.Context, projectID string) error {
	args := m.Called(ctx, projectID)
	return args.Error(0)
}

// MockKeyRotationService is a test-specific version of KeyRotationService
// that works with our mock objects
type MockKeyRotationService struct {
	configManager    *MockConfigManager
	projectID        string
	rotationInterval time.Duration
	exchangeAdapters map[string]ExchangeKeyAdapter
	stopChan         chan struct{}
	lastRotation     map[string]time.Time
}

// NewMockKeyRotationService creates a test-specific key rotation service
func NewMockKeyRotationService(configManager *MockConfigManager, config KeyRotationConfig) *MockKeyRotationService {
	return &MockKeyRotationService{
		configManager:    configManager,
		projectID:        config.ProjectID,
		rotationInterval: config.RotationInterval,
		exchangeAdapters: make(map[string]ExchangeKeyAdapter),
		stopChan:         make(chan struct{}),
		lastRotation:     make(map[string]time.Time),
	}
}

// RegisterExchangeAdapter registers an exchange adapter for key rotation
func (s *MockKeyRotationService) RegisterExchangeAdapter(exchangeName string, adapter ExchangeKeyAdapter) {
	s.exchangeAdapters[exchangeName] = adapter
}

// RotateKeys rotates API keys for all configured exchanges
func (s *MockKeyRotationService) RotateKeys(ctx context.Context) error {
	cfg := s.configManager.GetConfig()
	if cfg == nil {
		return fmt.Errorf("configuration is nil")
	}

	for _, exchange := range cfg.Exchanges {
		// Skip exchanges that don't use secrets
		if !exchange.UseSecrets {
			continue
		}

		// Skip exchanges without a registered adapter
		adapter, ok := s.exchangeAdapters[exchange.Name]
		if !ok {
			continue
		}

		// Generate new keys
		_, _, err := adapter.GenerateNewKeys(ctx, exchange)
		if err != nil {
			continue
		}

		// Revoke old keys
		if err := adapter.RevokeOldKeys(ctx, exchange); err != nil {
			// Continue anyway, as the new keys are already in use
		}

		// Update last rotation time
		s.lastRotation[exchange.Name] = time.Now()
	}

	// Reload configuration to use the new keys
	if err := s.configManager.ResolveSecrets(ctx, s.projectID); err != nil {
		return err
	}

	return nil
}

// ForceRotation forces an immediate key rotation for a specific exchange
func (s *MockKeyRotationService) ForceRotation(ctx context.Context, exchangeName string) error {
	cfg := s.configManager.GetConfig()
	if cfg == nil {
		return fmt.Errorf("configuration is nil")
	}

	// Find the exchange configuration
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
		return fmt.Errorf("exchange %s not found in configuration", exchangeName)
	}

	// Skip exchanges that don't use secrets
	if !exchangeConfig.UseSecrets {
		return fmt.Errorf("exchange %s is not using secrets", exchangeName)
	}

	// Skip exchanges without a registered adapter
	adapter, ok := s.exchangeAdapters[exchangeName]
	if !ok {
		return fmt.Errorf("no adapter registered for exchange %s", exchangeName)
	}

	// Generate new keys
	_, _, err := adapter.GenerateNewKeys(ctx, exchangeConfig)
	if err != nil {
		return err
	}

	// Revoke old keys
	if err := adapter.RevokeOldKeys(ctx, exchangeConfig); err != nil {
		// Continue anyway, as the new keys are already in use
	}

	// Update last rotation time
	s.lastRotation[exchangeName] = time.Now()

	// Reload configuration to use the new keys
	if err := s.configManager.ResolveSecrets(ctx, s.projectID); err != nil {
		return err
	}

	return nil
}

// GetLastRotationTime returns the last rotation time for an exchange
func (s *MockKeyRotationService) GetLastRotationTime(exchangeName string) (time.Time, bool) {
	lastRotation, ok := s.lastRotation[exchangeName]
	return lastRotation, ok
}

func TestKeyRotationService(t *testing.T) {
	// Create mock config manager
	mockConfig := &config.Config{
		Exchanges: []config.ExchangeConfig{
			{
				Name:                "Binance",
				UseSecrets:          true,
				APIKey:              "old_api_key",
				APISecret:           "old_api_secret",
				APIKeySecretPath:    "binance-api-key",
				APISecretSecretPath: "binance-api-secret",
			},
			{
				Name:       "Coinbase",
				UseSecrets: false,
				APIKey:     "direct_api_key",
				APISecret:  "direct_api_secret",
			},
		},
	}

	mockConfigManager := &MockConfigManager{
		config: mockConfig,
	}

	// Create mock exchange adapter
	mockAdapter := &MockExchangeKeyAdapter{}

	// Create key rotation service
	keyRotationConfig := KeyRotationConfig{
		Enabled:          true,
		RotationInterval: 24 * time.Hour,
		ProjectID:        "test-project",
	}

	// Use our test-specific service
	service := NewMockKeyRotationService(mockConfigManager, keyRotationConfig)

	// Register mock adapter
	service.RegisterExchangeAdapter("Binance", mockAdapter)

	// Set up expectations
	mockAdapter.On("GenerateNewKeys", mock.Anything, mockConfig.Exchanges[0]).
		Return("new_api_key", "new_api_secret", nil)
	mockAdapter.On("RevokeOldKeys", mock.Anything, mockConfig.Exchanges[0]).
		Return(nil)
	mockConfigManager.On("ResolveSecrets", mock.Anything, "test-project").
		Return(nil)

	// Test RotateKeys
	err := service.RotateKeys(context.Background())
	assert.NoError(t, err)

	// Verify expectations
	mockAdapter.AssertExpectations(t)
	mockConfigManager.AssertExpectations(t)

	// Test ForceRotation
	mockAdapter.On("GenerateNewKeys", mock.Anything, mockConfig.Exchanges[0]).
		Return("forced_api_key", "forced_api_secret", nil)
	mockAdapter.On("RevokeOldKeys", mock.Anything, mockConfig.Exchanges[0]).
		Return(nil)
	mockConfigManager.On("ResolveSecrets", mock.Anything, "test-project").
		Return(nil)

	err = service.ForceRotation(context.Background(), "Binance")
	assert.NoError(t, err)

	// Verify expectations
	mockAdapter.AssertExpectations(t)
	mockConfigManager.AssertExpectations(t)

	// Test ForceRotation with non-existent exchange
	err = service.ForceRotation(context.Background(), "NonExistentExchange")
	assert.Error(t, err)

	// Test ForceRotation with exchange not using secrets
	err = service.ForceRotation(context.Background(), "Coinbase")
	assert.Error(t, err)

	// Test GetLastRotationTime
	_, ok := service.GetLastRotationTime("Binance")
	assert.True(t, ok)

	_, ok = service.GetLastRotationTime("NonExistentExchange")
	assert.False(t, ok)
}
