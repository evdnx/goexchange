# Security Hardening

This package provides security hardening features for the CryptoBot application, including secure API key storage, encryption for sensitive data, and secure configuration management.

## Overview

The security package includes the following components:

1. **Key Manager**: Securely stores and manages API keys and other sensitive credentials
2. **Encryption Service**: Provides encryption and decryption for sensitive data
3. **Secure Config Manager**: Manages encrypted configuration data
4. **PASETO Token Manager**: Provides secure token generation and validation using PASETO
5. **Security Middleware**: Provides secure authentication middleware for API endpoints

## Key Manager

The Key Manager provides secure storage and retrieval of API keys and other sensitive credentials. It uses AES-GCM encryption with a master key derived using PBKDF2.

### Features

- Secure storage of API keys and other sensitive credentials
- Encryption using AES-GCM with a master key derived using PBKDF2
- Persistent storage in an encrypted file
- Key rotation support

### Usage

```go
// Create a new Key Manager
keyManager, err := security.NewKeyManager("MASTER_KEY_ENV", "keys.enc")
if err != nil {
    log.Fatalf("Failed to create key manager: %v", err)
}

// Store an API key
err = keyManager.SetKey("binance-api-key", "your-api-key")
if err != nil {
    log.Fatalf("Failed to store API key: %v", err)
}

// Retrieve an API key
apiKey, err := keyManager.GetKey("binance-api-key")
if err != nil {
    log.Fatalf("Failed to retrieve API key: %v", err)
}

// Rotate an API key
err = keyManager.RotateKey("binance-api-key", "new-api-key")
if err != nil {
    log.Fatalf("Failed to rotate API key: %v", err)
}
```

## Encryption Service

The Encryption Service provides encryption and decryption for sensitive data using AES-GCM with a key derived using PBKDF2.

### Features

- Encryption and decryption of strings, maps, and files
- AES-GCM encryption with a key derived using PBKDF2
- Support for encrypting credentials, JSON, and other sensitive data

### Usage

```go
// Create a new Encryption Service
encryptionService, err := security.NewEncryptionService("ENCRYPTION_KEY_ENV")
if err != nil {
    log.Fatalf("Failed to create encryption service: %v", err)
}

// Encrypt a string
encrypted, err := encryptionService.Encrypt("sensitive data")
if err != nil {
    log.Fatalf("Failed to encrypt data: %v", err)
}

// Decrypt a string
decrypted, err := encryptionService.Decrypt(encrypted)
if err != nil {
    log.Fatalf("Failed to decrypt data: %v", err)
}

// Encrypt a map
encryptedMap, err := encryptionService.EncryptMap(map[string]string{
    "username": "user",
    "password": "pass",
})
if err != nil {
    log.Fatalf("Failed to encrypt map: %v", err)
}

// Encrypt a file
err = encryptionService.EncryptFile("config.json", "config.enc")
if err != nil {
    log.Fatalf("Failed to encrypt file: %v", err)
}
```

## Secure Config Manager

The Secure Config Manager provides secure storage and retrieval of configuration data. It uses the Encryption Service to encrypt configuration data before storing it to disk.

### Features

- Secure storage of configuration data
- Encryption of sensitive configuration values
- Support for storing and retrieving credentials
- Persistent storage in an encrypted file

### Usage

```go
// Create a new Secure Config Manager
configManager, err := security.NewSecureConfigManager(encryptionService, "config.enc")
if err != nil {
    log.Fatalf("Failed to create config manager: %v", err)
}

// Set a regular configuration value
err = configManager.Set("port", 8080)
if err != nil {
    log.Fatalf("Failed to set config value: %v", err)
}

// Set a secure configuration value
err = configManager.SetSecure("api-key", "your-api-key")
if err != nil {
    log.Fatalf("Failed to set secure config value: %v", err)
}

// Get a regular configuration value
port, ok := configManager.GetInt("port")
if !ok {
    log.Fatalf("Failed to get config value")
}

// Get a secure configuration value
apiKey, err := configManager.GetSecure("api-key")
if err != nil {
    log.Fatalf("Failed to get secure config value: %v", err)
}

// Store credentials
err = configManager.SetCredentials("database", "user", "pass")
if err != nil {
    log.Fatalf("Failed to set credentials: %v", err)
}

// Retrieve credentials
username, password, err := configManager.GetCredentials("database")
if err != nil {
    log.Fatalf("Failed to get credentials: %v", err)
}
```

## PASETO Token Manager

The PASETO Token Manager provides secure token generation and validation using the Platform-Agnostic Security Tokens (PASETO) standard. PASETO is a more secure alternative to JWT that avoids many of the security issues that can occur with JWT.

### Features

- Secure token generation and validation using PASETO v2
- Support for both local (symmetric) and public (asymmetric) tokens
- Encryption using XChaCha20-Poly1305
- Automatic validation of token claims

### Usage

```go
// Create a new PASETO Token Manager
tokenManager, err := security.NewPasetoTokenManager("SYMMETRIC_KEY_ENV", "ENCRYPTION_KEY_ENV", "issuer", "audience")
if err != nil {
    log.Fatalf("Failed to create token manager: %v", err)
}

// Generate a token
token, err := tokenManager.GenerateToken("user123", 24*time.Hour, map[string]interface{}{
    "role": "admin",
})
if err != nil {
    log.Fatalf("Failed to generate token: %v", err)
}

// Validate a token
claims, err := tokenManager.ValidateToken(token)
if err != nil {
    log.Fatalf("Failed to validate token: %v", err)
}
fmt.Printf("Subject: %s, Role: %s\n", claims.Subject, claims.Custom["role"])
```

## Security Middleware

The Security Middleware provides secure authentication middleware for API endpoints. It includes API key authentication and PASETO token authentication.

### Features

- API key authentication middleware
- PASETO token authentication middleware
- Support for custom authentication functions
- Context-based authentication

### Usage

```go
// Create a new API Key Middleware
apiKeyMiddleware := security.NewAPIKeyMiddleware(keyManager, "api-key")

// Use the middleware
http.Handle("/api/secure", apiKeyMiddleware.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
    fmt.Fprintf(w, "Secure endpoint")
})))

// Create a new Token Middleware
tokenManager, err := security.NewPasetoTokenManager("SYMMETRIC_KEY_ENV", "ENCRYPTION_KEY_ENV", "issuer", "audience")
if err != nil {
    log.Fatalf("Failed to create token manager: %v", err)
}
tokenMiddleware := security.NewTokenMiddleware(tokenManager)

// Generate a PASETO token
token, err := tokenMiddleware.GenerateToken("user123", 24*time.Hour, map[string]interface{}{
    "role": "admin",
})
if err != nil {
    log.Fatalf("Failed to generate token: %v", err)
}

// Use the token middleware
http.Handle("/api/token", tokenMiddleware.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
    claims, ok := security.GetTokenClaims(r.Context())
    if !ok {
        http.Error(w, "Failed to get token claims", http.StatusInternalServerError)
        return
    }
    fmt.Fprintf(w, "Hello, %s", claims.Subject)
})))
```

## Best Practices

1. **Environment Variables**: Store master keys and encryption keys in environment variables, not in code or configuration files.
2. **Key Rotation**: Regularly rotate API keys and other credentials.
3. **Secure Storage**: Use the Key Manager and Secure Config Manager to store sensitive data.
4. **Encryption**: Use the Encryption Service to encrypt sensitive data before storing it.
5. **Authentication**: Use the Security Middleware to secure API endpoints.
6. **HTTPS**: Always use HTTPS in production environments.
7. **Logging**: Be careful not to log sensitive data.
8. **Access Control**: Implement proper access control for sensitive operations.
9. **Token Security**: Use PASETO tokens instead of JWT for better security.

## Security Considerations

1. **Master Key**: The security of the entire system depends on the security of the master key. Keep it safe!
2. **Environment Variables**: Environment variables can be accessed by anyone with access to the process environment. Use a secure environment variable manager in production.
3. **Memory**: Sensitive data in memory can be exposed in core dumps or by memory-reading malware. Consider using secure memory techniques in high-security environments.
4. **Key Derivation**: The key derivation function (PBKDF2) uses a fixed salt in this implementation. In a production environment, consider using a secure random salt stored separately.
5. **Encryption**: AES-GCM and XChaCha20-Poly1305 are used for encryption, which are secure but require careful handling of nonces. The implementation generates a random nonce for each encryption operation.
6. **PASETO**: PASETO is used for token authentication, which is more secure than JWT. However, it still requires proper key management and secure storage of the symmetric key.