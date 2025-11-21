package config

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	secretmanagerpb "cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/fsnotify/fsnotify"
	"github.com/go-playground/validator/v10"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/viper"
)

// ConfigManager handles configuration loading, validation, and hot reloading
type ConfigManager struct {
	viper       *viper.Viper
	config      *Config
	configLock  sync.RWMutex
	validate    *validator.Validate
	watchConfig bool
	onChange    []func(config *Config)
}

// Config represents the application configuration with validation
type Config struct {
	LogLevel       string            `mapstructure:"logLevel" validate:"required,oneof=debug info warning error fatal"`
	Exchanges      []ExchangeConfig  `mapstructure:"exchanges" validate:"dive"`
	Strategies     []StrategyConfig  `mapstructure:"strategies" validate:"dive"`
	RiskManagement RiskConfig        `mapstructure:"riskManagement" validate:"required"`
	API            APIConfig         `mapstructure:"api" validate:"required"`
	Database       DatabaseConfig    `mapstructure:"database" validate:"required"`
	KeyRotation    KeyRotationConfig `mapstructure:"keyRotation"`
	Metrics        struct {
		Namespace string `mapstructure:"namespace"`
	} `mapstructure:"metrics"`
	Logging LoggingConfig `mapstructure:"logging"`
}

// LoggingConfig captures basic logging preferences for the service
type LoggingConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
	Output string `mapstructure:"output"`
}

// ExchangeConfig represents exchange-specific configuration with validation
type ExchangeConfig struct {
	Name       string `mapstructure:"name" validate:"required"`
	APIKey     string `mapstructure:"apiKey" validate:"required_if=UseSecrets false"`
	APISecret  string `mapstructure:"apiSecret" validate:"required_if=UseSecrets false"`
	Testnet    bool   `mapstructure:"testnet"`
	UseSecrets bool   `mapstructure:"useSecrets"`
	// GCP Secret Manager paths
	APIKeySecretPath    string `mapstructure:"apiKeySecretPath" validate:"required_if=UseSecrets true"`
	APISecretSecretPath string `mapstructure:"apiSecretSecretPath" validate:"required_if=UseSecrets true"`
}

// StrategyConfig represents strategy-specific configuration with validation and type safety
type StrategyConfig struct {
	Name       string             `mapstructure:"name" validate:"required"`
	Enabled    bool               `mapstructure:"enabled"`
	Exchange   string             `mapstructure:"exchange" validate:"required"`
	Symbol     string             `mapstructure:"symbol" validate:"required"`
	Parameters StrategyParameters `mapstructure:"parameters" validate:"required"`
}

// StrategyParameters is an interface that all strategy parameter types must implement
type StrategyParameters interface {
	Validate() error
	GetType() string
}

// MomentumParameters represents parameters specific to the Momentum strategy
type MomentumParameters struct {
	LookbackPeriod int     `mapstructure:"lookbackPeriod" validate:"required,gt=0"`
	Threshold      float64 `mapstructure:"threshold" validate:"required,gt=0"`
}

func (p MomentumParameters) Validate() error {
	validate := validator.New()
	return validate.Struct(p)
}

func (p MomentumParameters) GetType() string {
	return "Momentum"
}

// MeanReversionParameters represents parameters specific to the MeanReversion strategy
type MeanReversionParameters struct {
	LookbackPeriod     int     `mapstructure:"lookbackPeriod" validate:"required,gt=0"`
	DeviationThreshold float64 `mapstructure:"deviationThreshold" validate:"required,gt=0"`
}

func (p MeanReversionParameters) Validate() error {
	validate := validator.New()
	return validate.Struct(p)
}

func (p MeanReversionParameters) GetType() string {
	return "MeanReversion"
}

// GridParameters represents parameters specific to the Grid strategy
type GridParameters struct {
	UpperPrice      float64 `mapstructure:"upperPrice" validate:"required,gt=0"`
	LowerPrice      float64 `mapstructure:"lowerPrice" validate:"required,gt=0,lt=UpperPrice"`
	GridLevels      int     `mapstructure:"gridLevels" validate:"required,gt=1"`
	QuantityPerGrid float64 `mapstructure:"quantityPerGrid" validate:"required,gt=0"`
}

func (p GridParameters) Validate() error {
	validate := validator.New()
	return validate.Struct(p)
}

func (p GridParameters) GetType() string {
	return "Grid"
}

// RiskConfig represents risk management configuration with validation
type RiskConfig struct {
	MaxDrawdown     float64 `mapstructure:"maxDrawdown" validate:"required,gt=0,lte=1"`
	MaxPositionSize float64 `mapstructure:"maxPositionSize" validate:"required,gt=0"`
	StopLossPercent float64 `mapstructure:"stopLossPercent" validate:"required,gt=0,lte=1"`
}

// APIConfig represents API server configuration with validation
type APIConfig struct {
	Enabled    bool   `mapstructure:"enabled"`
	Port       int    `mapstructure:"port" validate:"required_if=Enabled true,gt=0,lt=65536"`
	Secret     string `mapstructure:"secret" validate:"required_if=Enabled true,required_if=UseSecrets false"`
	UseSecrets bool   `mapstructure:"useSecrets"`
	SecretPath string `mapstructure:"secretPath" validate:"required_if=UseSecrets true"`
}

// DatabaseOptimizationConfig represents database optimization configuration
type DatabaseOptimizationConfig struct {
	Enabled                 bool   `mapstructure:"enabled" validate:"required"`
	BatchSize               int    `mapstructure:"batch_size" validate:"required_if=Enabled true,gt=0"`
	EnablePartitioning      bool   `mapstructure:"enable_partitioning" validate:"required"`
	EnableMaterializedViews bool   `mapstructure:"enable_materialized_views" validate:"required"`
	QueryAnalysis           bool   `mapstructure:"query_analysis" validate:"required"`
	SlowQueryThreshold      int    `mapstructure:"slow_query_threshold" validate:"required_if=QueryAnalysis true,gt=0"`
	PartitionInterval       string `mapstructure:"partition_interval" validate:"required_if=EnablePartitioning true"`
}

// DatabaseConfig represents database configuration with validation
type DatabaseConfig struct {
	DriverName     string                     `mapstructure:"driver_name" validate:"required,oneof=sqlite3 postgres"`
	SQLite         SQLiteConfig               `mapstructure:"sqlite" validate:"required_if=DriverName sqlite3"`
	Postgres       PostgresConfig             `mapstructure:"postgres" validate:"required_if=DriverName postgres"`
	ConnectionPool ConnectionConfig           `mapstructure:"connection_pool" validate:"required"`
	Optimization   DatabaseOptimizationConfig `mapstructure:"optimization"`
}

// SQLiteConfig represents SQLite configuration with validation
type SQLiteConfig struct {
	Database string `mapstructure:"database" validate:"required"`
}

// PostgresConfig represents PostgreSQL configuration with validation
type PostgresConfig struct {
	Host               string `mapstructure:"host" validate:"required"`
	Port               int    `mapstructure:"port" validate:"required,gt=0,lt=65536"`
	Username           string `mapstructure:"username" validate:"required"`
	Password           string `mapstructure:"password"`
	Database           string `mapstructure:"database" validate:"required"`
	SSLMode            string `mapstructure:"ssl_mode" validate:"required,oneof=disable require verify-ca verify-full"`
	UseSecrets         bool   `mapstructure:"useSecrets"`
	PasswordSecretPath string `mapstructure:"passwordSecretPath" validate:"required_if=UseSecrets true"`
}

// ConnectionConfig represents connection pool configuration with validation
type ConnectionConfig struct {
	MaxOpenConns           int `mapstructure:"max_open_conns" validate:"required,gt=0"`
	MaxIdleConns           int `mapstructure:"max_idle_conns" validate:"required,gt=0"`
	ConnMaxLifetimeSeconds int `mapstructure:"conn_max_lifetime_seconds" validate:"required,gt=0"`
}

// RepositoryConfig is a lightweight representation of database settings for consumers
type RepositoryConfig struct {
	DriverName      string
	Database        string
	Host            string
	Port            int
	Username        string
	Password        string
	SSLMode         string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
}

// KeyRotationConfig represents key rotation configuration with validation
type KeyRotationConfig struct {
	Enabled          bool          `mapstructure:"enabled"`
	RotationInterval time.Duration `mapstructure:"rotationInterval" validate:"required_if=Enabled true"`
	ProjectID        string        `mapstructure:"projectID" validate:"required_if=Enabled true"`
}

// NewConfigManager creates a new configuration manager
func NewConfigManager(configPath string, watchConfig bool) (*ConfigManager, error) {
	v := viper.New()
	v.SetConfigType("yaml") // Set YAML as the default format for all config files

	// Set up environment variable support
	v.SetEnvPrefix("CRYPTOBOT")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Load default configuration
	if err := loadDefaultConfig(v); err != nil {
		return nil, fmt.Errorf("failed to load default configuration: %w", err)
	}

	// Load configuration from file if provided
	if configPath != "" {
		absPath, err := filepath.Abs(configPath)
		if err != nil {
			return nil, fmt.Errorf("failed to get absolute path: %w", err)
		}

		v.SetConfigFile(absPath)
		if err := v.MergeInConfig(); err != nil {
			return nil, fmt.Errorf("failed to load configuration file: %w", err)
		}
	}

	// Create validator
	validate := validator.New()

	// Create config manager
	cm := &ConfigManager{
		viper:       v,
		validate:    validate,
		watchConfig: watchConfig,
		onChange:    make([]func(config *Config), 0),
	}

	// Load initial configuration
	if err := cm.loadConfig(); err != nil {
		return nil, err
	}

	// Set up hot reloading if enabled
	if watchConfig {
		v.WatchConfig()
		v.OnConfigChange(func(e fsnotify.Event) {
			if err := cm.loadConfig(); err != nil {
				fmt.Printf("Error reloading configuration: %v\n", err)
				return
			}

			// Notify all registered callbacks
			cm.configLock.RLock()
			defer cm.configLock.RUnlock()
			for _, callback := range cm.onChange {
				callback(cm.config)
			}
		})
	}

	return cm, nil
}

// loadDefaultConfig loads default configuration values
func loadDefaultConfig(v *viper.Viper) error {
	// Set default values
	v.SetDefault("logLevel", "info")
	v.SetDefault("api.enabled", false)
	v.SetDefault("api.port", 8080)
	v.SetDefault("database.driver_name", "sqlite3")
	v.SetDefault("database.sqlite.database", "goexchange.db")
	v.SetDefault("database.connection_pool.max_open_conns", 10)
	v.SetDefault("database.connection_pool.max_idle_conns", 5)
	v.SetDefault("database.connection_pool.conn_max_lifetime_seconds", 3600)
	v.SetDefault("riskManagement.maxDrawdown", 0.1)
	v.SetDefault("riskManagement.maxPositionSize", 1000)
	v.SetDefault("riskManagement.stopLossPercent", 0.05)

	return nil
}

// loadConfig loads the configuration from Viper into the config struct
func (cm *ConfigManager) loadConfig() error {
	var rawConfig Config

	// Decode configuration
	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			mapstructure.StringToTimeDurationHookFunc(),
			mapstructure.StringToSliceHookFunc(","),
			strategyParametersHook(),
		),
		Result:      &rawConfig,
		ErrorUnused: true,
	})
	if err != nil {
		return fmt.Errorf("failed to create decoder: %w", err)
	}

	if err := decoder.Decode(cm.viper.AllSettings()); err != nil {
		return fmt.Errorf("failed to decode configuration: %w", err)
	}

	// Validate configuration
	if err := cm.validate.Struct(rawConfig); err != nil {
		return fmt.Errorf("configuration validation failed: %w", err)
	}

	// Validate strategy parameters
	for _, strategy := range rawConfig.Strategies {
		if strategy.Parameters != nil {
			if err := strategy.Parameters.Validate(); err != nil {
				return fmt.Errorf("invalid parameters for strategy %s: %w", strategy.Name, err)
			}
		} else {
			return fmt.Errorf("missing parameters for strategy %s", strategy.Name)
		}
	}

	// Update configuration
	cm.configLock.Lock()
	cm.config = &rawConfig
	cm.configLock.Unlock()

	return nil
}

// strategyParametersHook returns a mapstructure.DecodeHookFunc that converts
// a map[string]interface{} to the appropriate StrategyParameters type
func strategyParametersHook() mapstructure.DecodeHookFunc {
	return func(f reflect.Type, t reflect.Type, data interface{}) (interface{}, error) {
		// Check if the target type is StrategyParameters
		if t != reflect.TypeOf((*StrategyParameters)(nil)).Elem() {
			return data, nil
		}

		// Check if the source type is map[string]interface{}
		if f.Kind() != reflect.Map {
			return data, fmt.Errorf("expected map[string]interface{}, got %v", f.Kind())
		}

		// Get the strategy name from the parent struct
		strategyName := ""
		if m, ok := data.(map[string]interface{}); ok {
			if name, ok := m["_strategyName"].(string); ok {
				strategyName = name
				delete(m, "_strategyName")
			}
		}

		// Create the appropriate parameters struct based on the strategy name
		var params StrategyParameters
		switch strategyName {
		case "Momentum":
			var p MomentumParameters
			if err := mapstructure.Decode(data, &p); err != nil {
				return nil, fmt.Errorf("failed to decode Momentum parameters: %w", err)
			}
			params = p
		case "MeanReversion":
			var p MeanReversionParameters
			if err := mapstructure.Decode(data, &p); err != nil {
				return nil, fmt.Errorf("failed to decode MeanReversion parameters: %w", err)
			}
			params = p
		case "Grid":
			var p GridParameters
			if err := mapstructure.Decode(data, &p); err != nil {
				return nil, fmt.Errorf("failed to decode Grid parameters: %w", err)
			}
			params = p
		default:
			return nil, fmt.Errorf("unknown strategy type: %s", strategyName)
		}

		return params, nil
	}
}

// GetConfig returns the current configuration
func (cm *ConfigManager) GetConfig() *Config {
	cm.configLock.RLock()
	defer cm.configLock.RUnlock()
	return cm.config
}

// GetViper returns the Viper instance
func (cm *ConfigManager) GetViper() *viper.Viper {
	return cm.viper
}

// RegisterOnChangeCallback registers a callback function to be called when the configuration changes
func (cm *ConfigManager) RegisterOnChangeCallback(callback func(config *Config)) {
	cm.configLock.Lock()
	defer cm.configLock.Unlock()
	cm.onChange = append(cm.onChange, callback)
}

// ResolveSecrets resolves secrets from GCP Secret Manager
func (cm *ConfigManager) ResolveSecrets(ctx context.Context, projectID string) error {
	// Create a client
	client, err := secretmanager.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create secretmanager client: %w", err)
	}
	defer client.Close()

	cm.configLock.Lock()
	defer cm.configLock.Unlock()

	// Resolve exchange secrets
	for i, exchange := range cm.config.Exchanges {
		if exchange.UseSecrets {
			// Resolve API key
			apiKey, err := accessSecret(ctx, client, projectID, exchange.APIKeySecretPath)
			if err != nil {
				return fmt.Errorf("failed to access API key secret for exchange %s: %w", exchange.Name, err)
			}
			cm.config.Exchanges[i].APIKey = apiKey

			// Resolve API secret
			apiSecret, err := accessSecret(ctx, client, projectID, exchange.APISecretSecretPath)
			if err != nil {
				return fmt.Errorf("failed to access API secret for exchange %s: %w", exchange.Name, err)
			}
			cm.config.Exchanges[i].APISecret = apiSecret
		}
	}

	// Resolve API secret
	if cm.config.API.Enabled && cm.config.API.UseSecrets {
		apiSecret, err := accessSecret(ctx, client, projectID, cm.config.API.SecretPath)
		if err != nil {
			return fmt.Errorf("failed to access API secret: %w", err)
		}
		cm.config.API.Secret = apiSecret
	}

	// Resolve database secrets
	if cm.config.Database.DriverName == "postgres" && cm.config.Database.Postgres.UseSecrets {
		password, err := accessSecret(ctx, client, projectID, cm.config.Database.Postgres.PasswordSecretPath)
		if err != nil {
			return fmt.Errorf("failed to access database password secret: %w", err)
		}
		cm.config.Database.Postgres.Password = password
	}

	return nil
}

// accessSecret accesses a secret version from GCP Secret Manager
func accessSecret(ctx context.Context, client *secretmanager.Client, projectID, secretPath string) (string, error) {
	// Build the resource name of the secret version
	name := fmt.Sprintf("projects/%s/secrets/%s/versions/latest", projectID, secretPath)

	// Access the secret version
	req := &secretmanagerpb.AccessSecretVersionRequest{
		Name: name,
	}
	result, err := client.AccessSecretVersion(ctx, req)
	if err != nil {
		return "", fmt.Errorf("failed to access secret version: %w", err)
	}

	// Return the secret payload as a string
	return string(result.Payload.Data), nil
}

// ToRepositoryConfig converts the database configuration to a RepositoryConfig
func (c *DatabaseConfig) ToRepositoryConfig() RepositoryConfig {
	repoConfig := RepositoryConfig{
		DriverName:      c.DriverName,
		MaxOpenConns:    c.ConnectionPool.MaxOpenConns,
		MaxIdleConns:    c.ConnectionPool.MaxIdleConns,
		ConnMaxLifetime: time.Duration(c.ConnectionPool.ConnMaxLifetimeSeconds) * time.Second,
	}

	// Set driver-specific configuration
	switch c.DriverName {
	case "sqlite3":
		repoConfig.Database = c.SQLite.Database
	case "postgres":
		repoConfig.Host = c.Postgres.Host
		repoConfig.Port = c.Postgres.Port
		repoConfig.Username = c.Postgres.Username
		repoConfig.Password = c.Postgres.Password
		repoConfig.Database = c.Postgres.Database
		repoConfig.SSLMode = c.Postgres.SSLMode
	}

	return repoConfig
}
