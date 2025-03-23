package config

import (
	"github.com/ozgen/raven-mq/internal/utils"

	"github.com/joho/godotenv"
)

type Config struct {
	LogDir                 string
	DBFile                 string
	LogLevel               string
	RetryIntervalInSeconds int
	MaxRetryMessageCount   int
}

var Envs = initConfig()

func initConfig() Config {

	//load env variables
	godotenv.Load()

	return Config{
		LogDir:                 utils.GetEnv("RAVENMQ_LOG_DIR", "logs"),
		DBFile:                 utils.GetEnv("DB_FILE", "ravenmq.db"),
		LogLevel:               utils.GetEnv("LOG_LEVEL", "INFO"),
		RetryIntervalInSeconds: utils.GetEnvAsInt("RAVENMQ_RETRY_MAX_INTERVAL", 30),
		MaxRetryMessageCount:   utils.GetEnvAsInt("RAVENMQ_RETRY_MAX_RETRY_COUNT", 5),
	}
}
