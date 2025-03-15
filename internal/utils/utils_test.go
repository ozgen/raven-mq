package utils

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetEnv(t *testing.T) {
	const envKey = "TEST_ENV_VAR"
	const defaultValue = "default"

	os.Setenv(envKey, "exists")
	assert.Equal(t, "exists", GetEnv(envKey, defaultValue))

	os.Unsetenv(envKey)
	assert.Equal(t, defaultValue, GetEnv(envKey, defaultValue))
}

func Test_GetEnvAsInt_withValidData_toGetVariable(t *testing.T) {
	key := "TEST_INT"
	value := "42"
	fallback := 10
	os.Setenv(key, value)
	defer os.Unsetenv(key)

	got := GetEnvAsInt(key, fallback)
	if got != 42 {
		t.Errorf("GetEnvAsInt(%q, %d) = %d; want %d", key, fallback, got, 42)
	}
}

func Test_GetEnvAsInt_withInvalidData_toGetFallback(t *testing.T) {
	key := "TEST_INVALID_INT"
	value := "notanumber"
	fallback := 10
	os.Setenv(key, value)
	defer os.Unsetenv(key)

	got := GetEnvAsInt(key, fallback)
	if got != fallback {
		t.Errorf("GetEnvAsInt(%q, %d) = %d; want %d", key, fallback, got, fallback)
	}
}

func Test_GetEnvAsInt_withMissingKey_toGetFallback(t *testing.T) {
	key := "TEST_MISSING"
	fallback := 10

	got := GetEnvAsInt(key, fallback)
	if got != fallback {
		t.Errorf("GetEnvAsInt(%q, %d) = %d; want %d", key, fallback, got, fallback)
	}
}
