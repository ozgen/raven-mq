package config

import (
	"github.com/ozgen/raven-mq/internal/utils"

	"github.com/joho/godotenv"
)

type Config struct {
	LogDir     string
	DBPassword string
}

var Envs = initConfig()

func initConfig() Config {

	//load env variables
	godotenv.Load()
	return Config{
		LogDir: utils.GetEnv("RAVENMQ_LOG_DIR", "logs"),
	}
}
