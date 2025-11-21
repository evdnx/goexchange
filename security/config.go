package security

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// SecureConfigManager provides secure storage and retrieval of configuration data
type SecureConfigManager struct {
	encryptionService *EncryptionService
	configFile        string
	config            map[string]interface{}
	mu                sync.RWMutex
}

// NewSecureConfigManager creates a new SecureConfigManager
func NewSecureConfigManager(encryptionService *EncryptionService, configFile string) (*SecureConfigManager, error) {
	cm := &SecureConfigManager{
		encryptionService: encryptionService,
		configFile:        configFile,
		config:            make(map[string]interface{}),
	}

	// Create directory if it doesn't exist
	dir := filepath.Dir(configFile)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create config directory: %w", err)
	}

	// Load config if file exists
	if _, err := os.Stat(configFile); err == nil {
		if err := cm.Load(); err != nil {
			return nil, fmt.Errorf("failed to load config: %w", err)
		}
	}

	return cm, nil
}

// Set sets a configuration value
func (cm *SecureConfigManager) Set(key string, value interface{}) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.config[key] = value
	return cm.save()
}

// Get gets a configuration value
func (cm *SecureConfigManager) Get(key string) (interface{}, bool) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	value, ok := cm.config[key]
	return value, ok
}

// GetString gets a string configuration value
func (cm *SecureConfigManager) GetString(key string) (string, bool) {
	value, ok := cm.Get(key)
	if !ok {
		return "", false
	}

	strValue, ok := value.(string)
	return strValue, ok
}

// GetInt gets an int configuration value
func (cm *SecureConfigManager) GetInt(key string) (int, bool) {
	value, ok := cm.Get(key)
	if !ok {
		return 0, false
	}

	// Handle different numeric types
	switch v := value.(type) {
	case int:
		return v, true
	case float64:
		return int(v), true
	default:
		return 0, false
	}
}

// GetBool gets a boolean configuration value
func (cm *SecureConfigManager) GetBool(key string) (bool, bool) {
	value, ok := cm.Get(key)
	if !ok {
		return false, false
	}

	boolValue, ok := value.(bool)
	return boolValue, ok
}

// GetMap gets a map configuration value
func (cm *SecureConfigManager) GetMap(key string) (map[string]interface{}, bool) {
	value, ok := cm.Get(key)
	if !ok {
		return nil, false
	}

	mapValue, ok := value.(map[string]interface{})
	return mapValue, ok
}

// Delete deletes a configuration value
func (cm *SecureConfigManager) Delete(key string) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	delete(cm.config, key)
	return cm.save()
}

// SetSecure sets a securely encrypted configuration value
func (cm *SecureConfigManager) SetSecure(key, value string) error {
	// Encrypt the value
	encryptedValue, err := cm.encryptionService.Encrypt(value)
	if err != nil {
		return fmt.Errorf("failed to encrypt value: %w", err)
	}

	// Store with a prefix to indicate it's encrypted
	return cm.Set(key, fmt.Sprintf("ENC:%s", encryptedValue))
}

// GetSecure gets a securely encrypted configuration value
func (cm *SecureConfigManager) GetSecure(key string) (string, error) {
	// Get the encrypted value
	value, ok := cm.GetString(key)
	if !ok {
		return "", fmt.Errorf("key %s not found", key)
	}

	// Check if the value is encrypted
	if !isEncrypted(value) {
		return "", fmt.Errorf("value for key %s is not encrypted", key)
	}

	// Extract the encrypted part
	encryptedValue := value[4:] // Remove "ENC:" prefix

	// Decrypt the value
	decryptedValue, err := cm.encryptionService.Decrypt(encryptedValue)
	if err != nil {
		return "", fmt.Errorf("failed to decrypt value: %w", err)
	}

	return decryptedValue, nil
}

// SetCredentials securely stores credentials
func (cm *SecureConfigManager) SetCredentials(key, username, password string) error {
	// Encrypt the credentials
	encryptedCreds, err := cm.encryptionService.EncryptCredentials(username, password)
	if err != nil {
		return fmt.Errorf("failed to encrypt credentials: %w", err)
	}

	// Store with a prefix to indicate it's encrypted credentials
	return cm.Set(key, fmt.Sprintf("CRED:%s", encryptedCreds))
}

// GetCredentials retrieves securely stored credentials
func (cm *SecureConfigManager) GetCredentials(key string) (string, string, error) {
	// Get the encrypted value
	value, ok := cm.GetString(key)
	if !ok {
		return "", "", fmt.Errorf("key %s not found", key)
	}

	// Check if the value is encrypted credentials
	if !isCredentials(value) {
		return "", "", fmt.Errorf("value for key %s is not encrypted credentials", key)
	}

	// Extract the encrypted part
	encryptedValue := value[5:] // Remove "CRED:" prefix

	// Decrypt the credentials
	username, password, err := cm.encryptionService.DecryptCredentials(encryptedValue)
	if err != nil {
		return "", "", fmt.Errorf("failed to decrypt credentials: %w", err)
	}

	return username, password, nil
}

// Load loads the configuration from file
func (cm *SecureConfigManager) Load() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Read the encrypted file
	encryptedData, err := os.ReadFile(cm.configFile)
	if err != nil {
		return fmt.Errorf("failed to read config file: %w", err)
	}

	// Decrypt the file
	decryptedData, err := cm.encryptionService.Decrypt(string(encryptedData))
	if err != nil {
		return fmt.Errorf("failed to decrypt config file: %w", err)
	}

	// Unmarshal the JSON
	var config map[string]interface{}
	if err := json.Unmarshal([]byte(decryptedData), &config); err != nil {
		return fmt.Errorf("failed to unmarshal config: %w", err)
	}

	cm.config = config
	return nil
}

// Save saves the configuration to file
func (cm *SecureConfigManager) Save() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	return cm.save()
}

// save is an internal method to save the configuration
func (cm *SecureConfigManager) save() error {
	// Add metadata
	configWithMeta := map[string]interface{}{
		"timestamp": time.Now().Unix(),
		"config":    cm.config,
	}

	// Marshal the config to JSON
	data, err := json.MarshalIndent(configWithMeta, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	// Encrypt the JSON
	encryptedData, err := cm.encryptionService.Encrypt(string(data))
	if err != nil {
		return fmt.Errorf("failed to encrypt config: %w", err)
	}

	// Write to file
	if err := os.WriteFile(cm.configFile, []byte(encryptedData), 0600); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}

// isEncrypted checks if a value is encrypted
func isEncrypted(value string) bool {
	return len(value) > 4 && value[:4] == "ENC:"
}

// isCredentials checks if a value is encrypted credentials
func isCredentials(value string) bool {
	return len(value) > 5 && value[:5] == "CRED:"
}
