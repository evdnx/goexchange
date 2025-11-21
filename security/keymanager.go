package security

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"golang.org/x/crypto/pbkdf2"
)

// KeyManager provides secure storage and retrieval of API keys and other sensitive data
type KeyManager struct {
	masterKey    []byte
	encryptedMap map[string]string
	mu           sync.RWMutex
	keyFile      string
	salt         []byte
}

// NewKeyManager creates a new KeyManager with the given master key
func NewKeyManager(masterKeyEnv, keyFile string) (*KeyManager, error) {
	// Get master key from environment variable
	masterKeyStr := os.Getenv(masterKeyEnv)
	if masterKeyStr == "" {
		return nil, fmt.Errorf("master key environment variable %s not set", masterKeyEnv)
	}

	// Generate a fixed salt (in a real system, this would be stored securely)
	salt := []byte("goexchange-salt-fixed-value-change-in-production")

	// Derive a key from the master key using PBKDF2
	masterKey := pbkdf2.Key([]byte(masterKeyStr), salt, 10000, 32, sha256.New)

	km := &KeyManager{
		masterKey:    masterKey,
		encryptedMap: make(map[string]string),
		keyFile:      keyFile,
		salt:         salt,
	}

	// Load keys from file if it exists
	if _, err := os.Stat(keyFile); err == nil {
		if err := km.loadFromFile(); err != nil {
			return nil, fmt.Errorf("failed to load keys from file: %w", err)
		}
	}

	return km, nil
}

// SetKey securely stores a key
func (km *KeyManager) SetKey(name, value string) error {
	// Encrypt the value
	encryptedValue, err := km.encrypt(value)
	if err != nil {
		return fmt.Errorf("failed to encrypt value: %w", err)
	}

	// Store the encrypted value
	km.mu.Lock()
	km.encryptedMap[name] = encryptedValue
	km.mu.Unlock()

	// Save to file
	return km.saveToFile()
}

// GetKey retrieves a securely stored key
func (km *KeyManager) GetKey(name string) (string, error) {
	km.mu.RLock()
	encryptedValue, exists := km.encryptedMap[name]
	km.mu.RUnlock()

	if !exists {
		return "", fmt.Errorf("key %s not found", name)
	}

	// Decrypt the value
	value, err := km.decrypt(encryptedValue)
	if err != nil {
		return "", fmt.Errorf("failed to decrypt value: %w", err)
	}

	return value, nil
}

// DeleteKey removes a key from storage
func (km *KeyManager) DeleteKey(name string) error {
	km.mu.Lock()
	delete(km.encryptedMap, name)
	km.mu.Unlock()

	// Save to file
	return km.saveToFile()
}

// RotateKey rotates a key with a new value
func (km *KeyManager) RotateKey(name, newValue string) error {
	// Check if key exists
	km.mu.RLock()
	_, exists := km.encryptedMap[name]
	km.mu.RUnlock()

	if !exists {
		return fmt.Errorf("key %s not found", name)
	}

	// Set the new value
	return km.SetKey(name, newValue)
}

// ListKeys returns a list of all key names
func (km *KeyManager) ListKeys() []string {
	km.mu.RLock()
	defer km.mu.RUnlock()

	keys := make([]string, 0, len(km.encryptedMap))
	for k := range km.encryptedMap {
		keys = append(keys, k)
	}

	return keys
}

// encrypt encrypts a value using AES-GCM
func (km *KeyManager) encrypt(value string) (string, error) {
	// Create a new cipher block from the key
	block, err := aes.NewCipher(km.masterKey)
	if err != nil {
		return "", err
	}

	// Create a new GCM cipher
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	// Create a nonce
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}

	// Encrypt the value
	ciphertext := gcm.Seal(nonce, nonce, []byte(value), nil)

	// Return the encrypted value as a base64 string
	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

// decrypt decrypts a value using AES-GCM
func (km *KeyManager) decrypt(encryptedValue string) (string, error) {
	// Decode the base64 string
	ciphertext, err := base64.StdEncoding.DecodeString(encryptedValue)
	if err != nil {
		return "", err
	}

	// Create a new cipher block from the key
	block, err := aes.NewCipher(km.masterKey)
	if err != nil {
		return "", err
	}

	// Create a new GCM cipher
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	// Check if the ciphertext is valid
	if len(ciphertext) < gcm.NonceSize() {
		return "", errors.New("ciphertext too short")
	}

	// Extract the nonce and ciphertext
	nonce, ciphertext := ciphertext[:gcm.NonceSize()], ciphertext[gcm.NonceSize():]

	// Decrypt the value
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", err
	}

	return string(plaintext), nil
}

// saveToFile saves the encrypted keys to a file
func (km *KeyManager) saveToFile() error {
	// Create a map with metadata
	saveData := map[string]interface{}{
		"timestamp": time.Now().Unix(),
		"keys":      km.encryptedMap,
	}

	// Marshal the map to JSON
	data, err := json.Marshal(saveData)
	if err != nil {
		return fmt.Errorf("failed to marshal keys: %w", err)
	}

	// Encrypt the entire file
	encryptedData, err := km.encrypt(string(data))
	if err != nil {
		return fmt.Errorf("failed to encrypt file: %w", err)
	}

	// Write to file
	if err := os.WriteFile(km.keyFile, []byte(encryptedData), 0600); err != nil {
		return fmt.Errorf("failed to write keys to file: %w", err)
	}

	return nil
}

// loadFromFile loads the encrypted keys from a file
func (km *KeyManager) loadFromFile() error {
	// Read from file
	encryptedData, err := os.ReadFile(km.keyFile)
	if err != nil {
		return fmt.Errorf("failed to read keys from file: %w", err)
	}

	// Decrypt the file
	data, err := km.decrypt(string(encryptedData))
	if err != nil {
		return fmt.Errorf("failed to decrypt file: %w", err)
	}

	// Unmarshal the JSON
	var saveData map[string]interface{}
	if err := json.Unmarshal([]byte(data), &saveData); err != nil {
		return fmt.Errorf("failed to unmarshal keys: %w", err)
	}

	// Extract the keys
	keysMap, ok := saveData["keys"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("invalid keys format")
	}

	// Convert to string map
	km.mu.Lock()
	for k, v := range keysMap {
		if strVal, ok := v.(string); ok {
			km.encryptedMap[k] = strVal
		}
	}
	km.mu.Unlock()

	return nil
}
