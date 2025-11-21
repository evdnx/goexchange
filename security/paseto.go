package security

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/o1egl/paseto"
	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/pbkdf2"
)

// PasetoTokenManager provides methods for generating and validating PASETO tokens
type PasetoTokenManager struct {
	symmetricKey  []byte
	encryptionKey []byte
	issuer        string
	audience      string
}

// TokenClaims represents the claims in a PASETO token
type TokenClaims struct {
	Subject   string                 `json:"sub,omitempty"`
	Issuer    string                 `json:"iss,omitempty"`
	Audience  string                 `json:"aud,omitempty"`
	Jti       string                 `json:"jti,omitempty"`
	IssuedAt  time.Time              `json:"iat,omitempty"`
	NotBefore time.Time              `json:"nbf,omitempty"`
	ExpiredAt time.Time              `json:"exp,omitempty"`
	Custom    map[string]interface{} `json:"custom,omitempty"`
}

// NewPasetoTokenManager creates a new PasetoTokenManager
func NewPasetoTokenManager(symmetricKeyEnv, encryptionKeyEnv, issuer, audience string) (*PasetoTokenManager, error) {
	// Get symmetric key from environment variable
	symmetricKeyStr := getEnvOrDefault(symmetricKeyEnv, "")
	if symmetricKeyStr == "" {
		return nil, fmt.Errorf("symmetric key environment variable %s not set", symmetricKeyEnv)
	}

	// Get encryption key from environment variable
	encryptionKeyStr := getEnvOrDefault(encryptionKeyEnv, "")
	if encryptionKeyStr == "" {
		return nil, fmt.Errorf("encryption key environment variable %s not set", encryptionKeyEnv)
	}

	// Derive a key from the symmetric key
	symmetricKey := deriveKey(symmetricKeyStr, "paseto-symmetric-key", 32)

	// Derive a key from the encryption key
	encryptionKey := deriveKey(encryptionKeyStr, "paseto-encryption-key", chacha20poly1305.KeySize)

	return &PasetoTokenManager{
		symmetricKey:  symmetricKey,
		encryptionKey: encryptionKey,
		issuer:        issuer,
		audience:      audience,
	}, nil
}

// GenerateToken generates a PASETO token
func (m *PasetoTokenManager) GenerateToken(subject string, expiration time.Duration, custom map[string]interface{}) (string, error) {
	// Create a new PASETO instance
	p := paseto.NewV2()

	// Create the footer
	footer := map[string]interface{}{
		"kid": "key-1",
	}
	footerJSON, err := json.Marshal(footer)
	if err != nil {
		return "", fmt.Errorf("failed to marshal footer: %w", err)
	}

	// Create the claims
	now := time.Now()
	claims := TokenClaims{
		Subject:   subject,
		Issuer:    m.issuer,
		Audience:  m.audience,
		Jti:       fmt.Sprintf("%d", now.UnixNano()),
		IssuedAt:  now,
		NotBefore: now,
		ExpiredAt: now.Add(expiration),
		Custom:    custom,
	}

	// Generate the token
	token, err := p.Encrypt(m.symmetricKey, claims, footerJSON)
	if err != nil {
		return "", fmt.Errorf("failed to generate token: %w", err)
	}

	return token, nil
}

// ValidateToken validates a PASETO token
func (m *PasetoTokenManager) ValidateToken(token string) (*TokenClaims, error) {
	// Create a new PASETO instance
	p := paseto.NewV2()

	// Create a variable to hold the claims
	var claims TokenClaims

	// Create a variable to hold the footer
	var footer string

	// Decrypt and validate the token
	err := p.Decrypt(token, m.symmetricKey, &claims, &footer)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt token: %w", err)
	}

	// Validate the claims
	now := time.Now()
	if claims.ExpiredAt.Before(now) {
		return nil, fmt.Errorf("token expired")
	}
	if claims.NotBefore.After(now) {
		return nil, fmt.Errorf("token not valid yet")
	}
	if claims.Issuer != m.issuer {
		return nil, fmt.Errorf("invalid issuer")
	}
	if claims.Audience != m.audience {
		return nil, fmt.Errorf("invalid audience")
	}

	return &claims, nil
}

// GenerateLocalToken generates a local PASETO token (v2.local)
func (m *PasetoTokenManager) GenerateLocalToken(subject string, expiration time.Duration, custom map[string]interface{}) (string, error) {
	// Create a new PASETO instance
	p := paseto.NewV2()

	// Create the footer
	footer := map[string]interface{}{
		"kid": "key-1",
		"typ": "v2.local",
	}
	footerJSON, err := json.Marshal(footer)
	if err != nil {
		return "", fmt.Errorf("failed to marshal footer: %w", err)
	}

	// Create the claims
	now := time.Now()
	claims := TokenClaims{
		Subject:   subject,
		Issuer:    m.issuer,
		Audience:  m.audience,
		Jti:       fmt.Sprintf("%d", now.UnixNano()),
		IssuedAt:  now,
		NotBefore: now,
		ExpiredAt: now.Add(expiration),
		Custom:    custom,
	}

	// Generate the token
	token, err := p.Encrypt(m.symmetricKey, claims, footerJSON)
	if err != nil {
		return "", fmt.Errorf("failed to generate token: %w", err)
	}

	return token, nil
}

// GeneratePublicToken generates a public PASETO token (v2.public)
func (m *PasetoTokenManager) GeneratePublicToken(subject string, expiration time.Duration, custom map[string]interface{}, privateKey []byte) (string, error) {
	// Create a new PASETO instance
	p := paseto.NewV2()

	// Create the footer
	footer := map[string]interface{}{
		"kid": "key-1",
		"typ": "v2.public",
	}
	footerJSON, err := json.Marshal(footer)
	if err != nil {
		return "", fmt.Errorf("failed to marshal footer: %w", err)
	}

	// Create the claims
	now := time.Now()
	claims := TokenClaims{
		Subject:   subject,
		Issuer:    m.issuer,
		Audience:  m.audience,
		Jti:       fmt.Sprintf("%d", now.UnixNano()),
		IssuedAt:  now,
		NotBefore: now,
		ExpiredAt: now.Add(expiration),
		Custom:    custom,
	}

	// Generate the token
	token, err := p.Sign(privateKey, claims, footerJSON)
	if err != nil {
		return "", fmt.Errorf("failed to generate token: %w", err)
	}

	return token, nil
}

// ValidatePublicToken validates a public PASETO token (v2.public)
func (m *PasetoTokenManager) ValidatePublicToken(token string, publicKey []byte) (*TokenClaims, error) {
	// Create a new PASETO instance
	p := paseto.NewV2()

	// Create a variable to hold the claims
	var claims TokenClaims

	// Create a variable to hold the footer
	var footer string

	// Verify the token
	err := p.Verify(token, publicKey, &claims, &footer)
	if err != nil {
		return nil, fmt.Errorf("failed to verify token: %w", err)
	}

	// Validate the claims
	now := time.Now()
	if claims.ExpiredAt.Before(now) {
		return nil, fmt.Errorf("token expired")
	}
	if claims.NotBefore.After(now) {
		return nil, fmt.Errorf("token not valid yet")
	}
	if claims.Issuer != m.issuer {
		return nil, fmt.Errorf("invalid issuer")
	}
	if claims.Audience != m.audience {
		return nil, fmt.Errorf("invalid audience")
	}

	return &claims, nil
}

// EncryptData encrypts data using XChaCha20-Poly1305
func (m *PasetoTokenManager) EncryptData(data []byte) ([]byte, error) {
	// Create a new PASETO instance
	p := paseto.NewV2()

	// Encrypt the data
	encrypted, err := p.Encrypt(m.encryptionKey, data, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to encrypt data: %w", err)
	}

	return []byte(encrypted), nil
}

// DecryptData decrypts data using XChaCha20-Poly1305
func (m *PasetoTokenManager) DecryptData(encryptedData []byte) ([]byte, error) {
	// Create a new PASETO instance
	p := paseto.NewV2()

	// Create a variable to hold the decrypted data
	var decrypted []byte

	// Decrypt the data
	err := p.Decrypt(string(encryptedData), m.encryptionKey, &decrypted, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt data: %w", err)
	}

	return decrypted, nil
}

// Helper function to derive a key from a string
func deriveKey(keyStr, salt string, keySize int) []byte {
	return pbkdf2.Key([]byte(keyStr), []byte(salt), 10000, keySize, sha256.New)
}

// Helper function to get an environment variable or a default value
func getEnvOrDefault(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}
