package security

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"golang.org/x/crypto/pbkdf2"
)

// EncryptionService provides methods for encrypting and decrypting sensitive data
type EncryptionService struct {
	key []byte
}

// NewEncryptionService creates a new EncryptionService with the given key
func NewEncryptionService(keyEnv string) (*EncryptionService, error) {
	// Get encryption key from environment variable
	keyStr := os.Getenv(keyEnv)
	if keyStr == "" {
		return nil, fmt.Errorf("encryption key environment variable %s not set", keyEnv)
	}

	// Generate a fixed salt (in a real system, this would be stored securely)
	salt := []byte("goexchange-encryption-salt-fixed-value-change-in-production")

	// Derive a key from the encryption key using PBKDF2
	key := pbkdf2.Key([]byte(keyStr), salt, 10000, 32, sha256.New)

	return &EncryptionService{
		key: key,
	}, nil
}

// Encrypt encrypts a string using AES-GCM
func (s *EncryptionService) Encrypt(plaintext string) (string, error) {
	// Create a new cipher block from the key
	block, err := aes.NewCipher(s.key)
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

	// Encrypt the plaintext
	ciphertext := gcm.Seal(nonce, nonce, []byte(plaintext), nil)

	// Return the encrypted value as a base64 string
	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

// Decrypt decrypts a string using AES-GCM
func (s *EncryptionService) Decrypt(ciphertextStr string) (string, error) {
	// Decode the base64 string
	ciphertext, err := base64.StdEncoding.DecodeString(ciphertextStr)
	if err != nil {
		return "", err
	}

	// Create a new cipher block from the key
	block, err := aes.NewCipher(s.key)
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

// EncryptMap encrypts all values in a map
func (s *EncryptionService) EncryptMap(data map[string]string) (map[string]string, error) {
	result := make(map[string]string)
	for k, v := range data {
		encrypted, err := s.Encrypt(v)
		if err != nil {
			return nil, fmt.Errorf("failed to encrypt value for key %s: %w", k, err)
		}
		result[k] = encrypted
	}
	return result, nil
}

// DecryptMap decrypts all values in a map
func (s *EncryptionService) DecryptMap(data map[string]string) (map[string]string, error) {
	result := make(map[string]string)
	for k, v := range data {
		decrypted, err := s.Decrypt(v)
		if err != nil {
			return nil, fmt.Errorf("failed to decrypt value for key %s: %w", k, err)
		}
		result[k] = decrypted
	}
	return result, nil
}

// EncryptJSON encrypts a JSON string
func (s *EncryptionService) EncryptJSON(jsonStr string) (string, error) {
	return s.Encrypt(jsonStr)
}

// DecryptJSON decrypts a JSON string
func (s *EncryptionService) DecryptJSON(encryptedJSON string) (string, error) {
	return s.Decrypt(encryptedJSON)
}

// EncryptCredentials encrypts credentials in the format "username:password"
func (s *EncryptionService) EncryptCredentials(username, password string) (string, error) {
	credentials := fmt.Sprintf("%s:%s", username, password)
	return s.Encrypt(credentials)
}

// DecryptCredentials decrypts credentials in the format "username:password"
func (s *EncryptionService) DecryptCredentials(encryptedCredentials string) (string, string, error) {
	decrypted, err := s.Decrypt(encryptedCredentials)
	if err != nil {
		return "", "", err
	}

	parts := strings.SplitN(decrypted, ":", 2)
	if len(parts) != 2 {
		return "", "", errors.New("invalid credentials format")
	}

	return parts[0], parts[1], nil
}

// EncryptFile encrypts a file
func (s *EncryptionService) EncryptFile(inputPath, outputPath string) error {
	// Read the input file
	plaintext, err := os.ReadFile(inputPath)
	if err != nil {
		return fmt.Errorf("failed to read input file: %w", err)
	}

	// Encrypt the file contents
	encrypted, err := s.Encrypt(string(plaintext))
	if err != nil {
		return fmt.Errorf("failed to encrypt file: %w", err)
	}

	// Write the encrypted contents to the output file
	if err := os.WriteFile(outputPath, []byte(encrypted), 0600); err != nil {
		return fmt.Errorf("failed to write output file: %w", err)
	}

	return nil
}

// DecryptFile decrypts a file
func (s *EncryptionService) DecryptFile(inputPath, outputPath string) error {
	// Read the input file
	encryptedData, err := os.ReadFile(inputPath)
	if err != nil {
		return fmt.Errorf("failed to read input file: %w", err)
	}

	// Decrypt the file contents
	decrypted, err := s.Decrypt(string(encryptedData))
	if err != nil {
		return fmt.Errorf("failed to decrypt file: %w", err)
	}

	// Write the decrypted contents to the output file
	if err := os.WriteFile(outputPath, []byte(decrypted), 0600); err != nil {
		return fmt.Errorf("failed to write output file: %w", err)
	}

	return nil
}
