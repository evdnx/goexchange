package security_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/evdnx/goexchange/security"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKeyManager(t *testing.T) {
	// Set up test environment
	os.Setenv("TEST_MASTER_KEY", "test-master-key")
	tempFile := "test_keys.enc"
	defer os.Remove(tempFile)

	// Create a key manager
	keyManager, err := security.NewKeyManager("TEST_MASTER_KEY", tempFile)
	require.NoError(t, err)

	// Test setting and getting a key
	err = keyManager.SetKey("test-key", "test-value")
	require.NoError(t, err)

	value, err := keyManager.GetKey("test-key")
	require.NoError(t, err)
	assert.Equal(t, "test-value", value)

	// Test rotating a key
	err = keyManager.RotateKey("test-key", "new-value")
	require.NoError(t, err)

	value, err = keyManager.GetKey("test-key")
	require.NoError(t, err)
	assert.Equal(t, "new-value", value)

	// Test deleting a key
	err = keyManager.DeleteKey("test-key")
	require.NoError(t, err)

	_, err = keyManager.GetKey("test-key")
	assert.Error(t, err)

	// Test listing keys
	err = keyManager.SetKey("key1", "value1")
	require.NoError(t, err)
	err = keyManager.SetKey("key2", "value2")
	require.NoError(t, err)

	keys := keyManager.ListKeys()
	assert.Len(t, keys, 2)
	assert.Contains(t, keys, "key1")
	assert.Contains(t, keys, "key2")
}

func TestEncryptionService(t *testing.T) {
	// Set up test environment
	os.Setenv("TEST_ENCRYPTION_KEY", "test-encryption-key")

	// Create an encryption service
	encryptionService, err := security.NewEncryptionService("TEST_ENCRYPTION_KEY")
	require.NoError(t, err)

	// Test encrypting and decrypting a string
	plaintext := "sensitive data"
	encrypted, err := encryptionService.Encrypt(plaintext)
	require.NoError(t, err)
	assert.NotEqual(t, plaintext, encrypted)

	decrypted, err := encryptionService.Decrypt(encrypted)
	require.NoError(t, err)
	assert.Equal(t, plaintext, decrypted)

	// Test encrypting and decrypting a map
	plaintextMap := map[string]string{
		"username": "user",
		"password": "pass",
	}
	encryptedMap, err := encryptionService.EncryptMap(plaintextMap)
	require.NoError(t, err)
	for k, v := range encryptedMap {
		assert.NotEqual(t, plaintextMap[k], v)
	}

	decryptedMap, err := encryptionService.DecryptMap(encryptedMap)
	require.NoError(t, err)
	assert.Equal(t, plaintextMap, decryptedMap)

	// Test encrypting and decrypting credentials
	username := "admin"
	password := "secure-password"
	encryptedCreds, err := encryptionService.EncryptCredentials(username, password)
	require.NoError(t, err)

	decryptedUser, decryptedPass, err := encryptionService.DecryptCredentials(encryptedCreds)
	require.NoError(t, err)
	assert.Equal(t, username, decryptedUser)
	assert.Equal(t, password, decryptedPass)

	// Test encrypting and decrypting a file
	tempFile := "test_file.txt"
	encryptedFile := "test_file.enc"
	defer os.Remove(tempFile)
	defer os.Remove(encryptedFile)

	err = os.WriteFile(tempFile, []byte("test file content"), 0600)
	require.NoError(t, err)

	err = encryptionService.EncryptFile(tempFile, encryptedFile)
	require.NoError(t, err)

	decryptedFile := "test_file_decrypted.txt"
	defer os.Remove(decryptedFile)

	err = encryptionService.DecryptFile(encryptedFile, decryptedFile)
	require.NoError(t, err)

	decryptedContent, err := os.ReadFile(decryptedFile)
	require.NoError(t, err)
	assert.Equal(t, "test file content", string(decryptedContent))
}

func TestSecureConfigManager(t *testing.T) {
	// Set up test environment
	os.Setenv("TEST_ENCRYPTION_KEY", "test-encryption-key")
	tempFile := "test_config.enc"
	defer os.Remove(tempFile)

	// Create an encryption service
	encryptionService, err := security.NewEncryptionService("TEST_ENCRYPTION_KEY")
	require.NoError(t, err)

	// Create a secure config manager
	configManager, err := security.NewSecureConfigManager(encryptionService, tempFile)
	require.NoError(t, err)

	// Test setting and getting a regular value
	err = configManager.Set("port", 8080)
	require.NoError(t, err)

	port, ok := configManager.GetInt("port")
	require.True(t, ok)
	assert.Equal(t, 8080, port)

	// Test setting and getting a secure value
	err = configManager.SetSecure("api-key", "secret-api-key")
	require.NoError(t, err)

	apiKey, err := configManager.GetSecure("api-key")
	require.NoError(t, err)
	assert.Equal(t, "secret-api-key", apiKey)

	// Test setting and getting credentials
	err = configManager.SetCredentials("database", "db-user", "db-pass")
	require.NoError(t, err)

	username, password, err := configManager.GetCredentials("database")
	require.NoError(t, err)
	assert.Equal(t, "db-user", username)
	assert.Equal(t, "db-pass", password)

	// Test saving and loading
	err = configManager.Save()
	require.NoError(t, err)

	// Create a new config manager to load the saved configuration
	newConfigManager, err := security.NewSecureConfigManager(encryptionService, tempFile)
	require.NoError(t, err)

	// Test that the values were loaded correctly
	port, ok = newConfigManager.GetInt("port")
	require.True(t, ok)
	assert.Equal(t, 8080, port)

	apiKey, err = newConfigManager.GetSecure("api-key")
	require.NoError(t, err)
	assert.Equal(t, "secret-api-key", apiKey)

	username, password, err = newConfigManager.GetCredentials("database")
	require.NoError(t, err)
	assert.Equal(t, "db-user", username)
	assert.Equal(t, "db-pass", password)
}

func TestAPIKeyMiddleware(t *testing.T) {
	// Set up test environment
	os.Setenv("TEST_MASTER_KEY", "test-master-key")
	tempFile := "test_keys.enc"
	defer os.Remove(tempFile)

	// Create a key manager
	keyManager, err := security.NewKeyManager("TEST_MASTER_KEY", tempFile)
	require.NoError(t, err)

	// Store an API key
	err = keyManager.SetKey("api-key", "test-api-key")
	require.NoError(t, err)

	// Create an API key middleware
	apiKeyMiddleware := security.NewAPIKeyMiddleware(keyManager, "api-key")

	// Create a test handler
	testHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("success"))
	})

	// Apply the middleware
	handler := apiKeyMiddleware.Middleware(testHandler)

	// Test with valid API key
	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("X-API-Key", "test-api-key")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "success", rec.Body.String())

	// Test with invalid API key
	req = httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("X-API-Key", "invalid-api-key")
	rec = httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)

	// Test with missing API key
	req = httptest.NewRequest("GET", "/test", nil)
	rec = httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestTokenMiddleware(t *testing.T) {
	// Set up test environment
	os.Setenv("TEST_SYMMETRIC_KEY", "test-symmetric-key")
	os.Setenv("TEST_ENCRYPTION_KEY", "test-encryption-key")

	// Create a PASETO token manager
	tokenManager, err := security.NewPasetoTokenManager("TEST_SYMMETRIC_KEY", "TEST_ENCRYPTION_KEY", "test-issuer", "test-audience")
	require.NoError(t, err)

	// Create a token middleware
	tokenMiddleware := security.NewTokenMiddleware(tokenManager)

	// Generate a PASETO token
	token, err := tokenMiddleware.GenerateToken("test-user", 1*time.Hour, map[string]interface{}{
		"role": "admin",
	})
	require.NoError(t, err)

	// Validate the PASETO token
	claims, err := tokenMiddleware.ValidateToken(token)
	require.NoError(t, err)
	assert.Equal(t, "test-user", claims.Subject)
	assert.Equal(t, "test-issuer", claims.Issuer)
	assert.Equal(t, "test-audience", claims.Audience)
	assert.Equal(t, "admin", claims.Custom["role"])

	// Create a test handler
	testHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claims, ok := security.GetTokenClaims(r.Context())
		if !ok {
			http.Error(w, "Failed to get token claims", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(claims.Subject))
	})

	// Apply the middleware
	handler := tokenMiddleware.Middleware(testHandler)

	// Test with valid PASETO token
	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "test-user", rec.Body.String())

	// Test with invalid PASETO token
	req = httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", "Bearer invalid-token")
	rec = httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)

	// Test with missing PASETO token
	req = httptest.NewRequest("GET", "/test", nil)
	rec = httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestAPIKeyContext(t *testing.T) {
	// Create a context with an API key
	ctx := context.Background()
	ctx = security.WithAPIKey(ctx, "test-api-key")

	// Get the API key from the context
	apiKey, ok := security.GetAPIKey(ctx)
	require.True(t, ok)
	assert.Equal(t, "test-api-key", apiKey)

	// Test with a context that doesn't have an API key
	ctx = context.Background()
	apiKey, ok = security.GetAPIKey(ctx)
	assert.False(t, ok)
	assert.Equal(t, "", apiKey)
}

func TestTokenClaimsContext(t *testing.T) {
	// Set up test environment
	os.Setenv("TEST_SYMMETRIC_KEY", "test-symmetric-key")
	os.Setenv("TEST_ENCRYPTION_KEY", "test-encryption-key")

	// Create a PASETO token manager
	tokenManager, err := security.NewPasetoTokenManager("TEST_SYMMETRIC_KEY", "TEST_ENCRYPTION_KEY", "test-issuer", "test-audience")
	require.NoError(t, err)

	// Create a token middleware
	tokenMiddleware := security.NewTokenMiddleware(tokenManager)

	// Generate a PASETO token
	token, err := tokenMiddleware.GenerateToken("test-user", 1*time.Hour, map[string]interface{}{
		"role": "admin",
	})
	require.NoError(t, err)

	// Validate the PASETO token
	claims, err := tokenMiddleware.ValidateToken(token)
	require.NoError(t, err)

	// Create a context with token claims
	ctx := context.Background()
	ctx = context.WithValue(ctx, security.TokenClaimsContextKey, claims)

	// Get the token claims from the context
	contextClaims, ok := security.GetTokenClaims(ctx)
	require.True(t, ok)
	assert.Equal(t, claims, contextClaims)

	// Test with a context that doesn't have token claims
	ctx = context.Background()
	contextClaims, ok = security.GetTokenClaims(ctx)
	assert.False(t, ok)
	assert.Nil(t, contextClaims)
}
