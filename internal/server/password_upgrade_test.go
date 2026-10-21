package server

import (
	"crypto/sha256"
	"encoding/hex"
	"golang.org/x/crypto/pbkdf2"
	"strings"
	"testing"
)

func TestPasswordHashUpgradeCompatibility(t *testing.T) {
	const password = "old-account-password"
	salt := []byte("0123456789abcdef")
	legacy := "$pbkdf2$" + hex.EncodeToString(salt) + "$" + hex.EncodeToString(pbkdf2.Key([]byte(password), salt, 10000, 32, sha256.New))
	if !VerifyPassword(password, legacy) || VerifyPassword("wrong", legacy) {
		t.Fatal("legacy hash verification failed")
	}
	hash, err := HashPassword(password)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(hash, "$pbkdf2$600000$") {
		t.Fatalf("new hash uses obsolete work factor: %s", hash)
	}
	if !VerifyPassword(password, hash) || VerifyPassword("wrong", hash) {
		t.Fatal("new hash verification failed")
	}
	for _, hash := range []string{"$pbkdf2$999999999999$00$00", "$pbkdf2$-1$00$00", "$pbkdf2$600000$00$00"} {
		if VerifyPassword(password, hash) {
			t.Error("malformed hash accepted")
		}
	}
}
