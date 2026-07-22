package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"math/big"

	"golang.org/x/crypto/bcrypt"
)

const (
	CollectorTokenPrefix = "qsc_"
	SessionTokenPrefix   = "qss_"
	tokenRandomBytes     = 20
	base62Base           = 62
	base62Alphabet       = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
)

// GenerateToken returns prefix followed by base62-encoded random bytes.
func GenerateToken(prefix string) (string, error) {
	buf := make([]byte, tokenRandomBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}

	return prefix + base62Encode(buf), nil
}

// HashToken returns the hex-encoded SHA-256 of an opaque token, the form stored in the database.
func HashToken(token string) string {
	sum := sha256.Sum256([]byte(token))

	return hex.EncodeToString(sum[:])
}

// HashPassword bcrypt-hashes a plaintext password.
func HashPassword(password string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}

	return string(hash), nil
}

// CheckPassword reports whether password matches a bcrypt hash.
func CheckPassword(hash, password string) bool {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) == nil
}

// base62Encode renders bytes as a big-endian base62 number.
func base62Encode(buf []byte) string {
	n := new(big.Int).SetBytes(buf)
	if n.Sign() == 0 {
		return base62Alphabet[:1]
	}

	base := big.NewInt(base62Base)
	zero := new(big.Int)
	mod := new(big.Int)

	var out []byte
	for n.Cmp(zero) > 0 {
		n.DivMod(n, base, mod)
		out = append(out, base62Alphabet[mod.Int64()])
	}

	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}

	return string(out)
}
