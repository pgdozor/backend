package config

import (
	"reflect"
	"testing"
)

func TestParseListenAddr(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		raw     string
		want    string
		wantErr bool
	}{
		{"empty defaults", "", defaultListenAddr, false},
		{"host and port", "localhost:3000", "localhost:3000", false},
		{"all interfaces", "0.0.0.0:3000", "0.0.0.0:3000", false},
		{"bare port", ":3000", ":3000", false},
		{"missing port", "localhost", "", true},
		{"too many colons", "a:b:c", "", true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseListenAddr(c.raw)
			if (err != nil) != c.wantErr {
				t.Fatalf("parseListenAddr(%q) error = %v, wantErr %v", c.raw, err, c.wantErr)
			}
			if !c.wantErr && got != c.want {
				t.Errorf("parseListenAddr(%q) = %q, want %q", c.raw, got, c.want)
			}
		})
	}
}

func TestParseRetentionDays(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		raw     string
		want    int
		wantErr bool
	}{
		{"empty defaults", "", defaultRetentionDays, false},
		{"at minimum", "14", minRetentionDays, false},
		{"above minimum", "45", 45, false},
		{"below minimum clamps up", "7", minRetentionDays, false},
		{"zero disables", "0", 0, false},
		{"negative rejected", "-1", 0, true},
		{"not a number", "soon", 0, true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseRetentionDays(c.raw)
			if (err != nil) != c.wantErr {
				t.Fatalf("parseRetentionDays(%q) error = %v, wantErr %v", c.raw, err, c.wantErr)
			}
			if !c.wantErr && got != c.want {
				t.Errorf("parseRetentionDays(%q) = %d, want %d", c.raw, got, c.want)
			}
		})
	}
}

func TestParseAllowedOrigins(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		raw  string
		want []string
	}{
		{"empty defaults", "", []string{defaultAllowedOrigin}},
		{"single", "https://app.example", []string{"https://app.example"}},
		{
			"multiple trimmed",
			" https://a.example , https://b.example ",
			[]string{"https://a.example", "https://b.example"},
		},
		{"blanks dropped", "https://a.example,, ,", []string{"https://a.example"}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			got := parseAllowedOrigins(c.raw)
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("parseAllowedOrigins(%q) = %v, want %v", c.raw, got, c.want)
			}
		})
	}
}
