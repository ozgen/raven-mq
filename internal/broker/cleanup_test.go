package broker

import (
	"os"
	"path/filepath"
	"testing"
)

func TestMain(m *testing.M) {
	os.Setenv("LOG_DIR", os.TempDir())
	os.Setenv("DB_FILE", filepath.Join(os.TempDir(), "ravenmq-test.db"))

	code := m.Run()

	os.Remove(filepath.Join(os.Getenv("LOG_DIR"), "raven-mq.log"))
	os.Remove(os.Getenv("DB_FILE"))

	os.Exit(code)
}
