package config

import (
	"fmt"
	"os"

	"github.com/spf13/viper"
)

type LogConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
}

type SQSConfig struct {
	QueueURL string `mapstructure:"queue_url"`
	Workers  int    `mapstructure:"workers"`
}

type Config struct {
	Log LogConfig `mapstructure:"log"`
	SQS SQSConfig `mapstructure:"sqs"`
}

func LoadConfig() (*Config, error) {
	configPath := os.Getenv("KV_VIPER_FILE")
	if configPath == "" {
		panic(fmt.Errorf("KV_VIPER_FILE env var is not set"))
	}
	viper.SetConfigFile(configPath)

	if err := viper.ReadInConfig(); err != nil {
		panic(fmt.Errorf("error reading config file: %s", err))
	}

	for _, key := range viper.AllKeys() {
		if val, ok := viper.Get(key).(string); ok {
			viper.Set(key, os.ExpandEnv(val))
		}
	}

	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("error unmarshalling config: %s", err))
	}

	return &config, nil
}
