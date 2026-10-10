package id

import (
	"testing"
	"time"
)

func TestNewIsValidVersion7(t *testing.T) {
	seen := map[string]bool{}
	for range 1000 {
		u := New()
		if !Valid(u) {
			t.Fatalf("New() = %q is not a valid UUID", u)
		}
		if u[14] != '7' {
			t.Fatalf("New() = %q is not version 7", u)
		}
		if c := u[19]; c != '8' && c != '9' && c != 'a' && c != 'b' {
			t.Fatalf("New() = %q has the wrong variant", u)
		}
		if seen[u] {
			t.Fatalf("duplicate id %q", u)
		}
		seen[u] = true
	}
}

func TestNewIsTimeOrdered(t *testing.T) {
	a := New()
	time.Sleep(2 * time.Millisecond)
	b := New()
	if !(a[:13] < b[:13]) {
		t.Fatalf("ids not ordered by time: %s then %s", a, b)
	}
}

func TestValid(t *testing.T) {
	for s, want := range map[string]bool{
		"0190a1b2-c3d4-7e5f-8a6b-7c8d9e0f1a2b": true,
		"0190A1B2-C3D4-7E5F-8A6B-7C8D9E0F1A2B": true,
		"0190a1b2c3d47e5f8a6b7c8d9e0f1a2b":     false,
		"0190a1b2-c3d4-7e5f-8a6b-7c8d9e0f1a2":  false,
		"0190a1b2-c3d4-7e5f-8a6b-7c8d9e0f1a2g": false,
		"":                                     false,
	} {
		if got := Valid(s); got != want {
			t.Errorf("Valid(%q) = %v, want %v", s, got, want)
		}
	}
}
