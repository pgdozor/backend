package auth

import (
	"strings"
	"testing"
)

func TestHashToken(t *testing.T) {
	t.Parallel()

	const (
		input = "pgdc_x"
		want  = "4c203fd60ae05d15fac3cc8e50dc47e6d2aa4b62d4295cfed34378d3a5d22b3b"
	)

	if got := HashToken(input); got != want {
		t.Fatalf("HashToken(%q) = %q, want %q", input, got, want)
	}

	if HashToken("pgdc_x") == HashToken("pgdc_y") {
		t.Fatal("HashToken collided on distinct inputs")
	}
}

func TestGenerateToken(t *testing.T) {
	t.Parallel()

	const attempts = 100

	seen := make(map[string]struct{}, attempts)
	for range attempts {
		token, err := GenerateToken(CollectorTokenPrefix)
		if err != nil {
			t.Fatalf("GenerateToken: %v", err)
		}

		body, ok := strings.CutPrefix(token, CollectorTokenPrefix)
		if !ok {
			t.Fatalf("token %q missing prefix %q", token, CollectorTokenPrefix)
		}
		if body == "" {
			t.Fatalf("token %q has empty body", token)
		}
		for _, r := range body {
			if !strings.ContainsRune(base62Alphabet, r) {
				t.Fatalf("token body %q has non-base62 rune %q", body, r)
			}
		}

		if _, dup := seen[token]; dup {
			t.Fatalf("GenerateToken produced a duplicate: %q", token)
		}
		seen[token] = struct{}{}
	}
}

func TestBase62Encode(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		in   []byte
		want string
	}{
		{"empty", []byte{}, "0"},
		{"all zero", []byte{0, 0, 0}, "0"},
		{"one", []byte{0, 0, 1}, "1"},
		{"last alphabet index", []byte{61}, "z"},
		{"base rollover", []byte{62}, "10"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			if got := base62Encode(c.in); got != c.want {
				t.Errorf("base62Encode(%v) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}

func TestHashPasswordRoundTrip(t *testing.T) {
	t.Parallel()

	const password = "s3cret-passphrase"

	hash, err := HashPassword(password)
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}

	if !CheckPassword(hash, password) {
		t.Error("CheckPassword rejected the correct password")
	}
	if CheckPassword(hash, "wrong-password") {
		t.Error("CheckPassword accepted a wrong password")
	}
	if CheckPassword("not-a-bcrypt-hash", password) {
		t.Error("CheckPassword accepted a malformed hash")
	}
}
