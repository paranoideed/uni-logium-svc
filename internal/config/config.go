package config

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/viper"
)

type RestConfig struct {
	Port         int           `mapstructure:"port"`
	ReadTimeout  time.Duration `mapstructure:"read_timeout"`
	WriteTimeout time.Duration `mapstructure:"write_timeout"`
	IdleTimeout  time.Duration `mapstructure:"idle_timeout"`
}

type LogConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
}

type SQSConfig struct {
	QueueURL          string        `mapstructure:"queue_url"`
	Workers           int           `mapstructure:"workers"`
	Fetchers          int           `mapstructure:"fetchers"`
	VisibilityTimeout time.Duration `mapstructure:"visibility_timeout"`
}

type Config struct {
	Log  LogConfig  `mapstructure:"log"`
	SQS  SQSConfig  `mapstructure:"sqs"`
	Rest RestConfig `mapstructure:"rest"`
}

func LoadConfig() (*Config, error) {
	configPath := os.Getenv("KV_VIPER_FILE")
	if configPath == "" {
		return nil, fmt.Errorf("KV_VIPER_FILE env var is not set")
	}
	viper.SetConfigFile(configPath)

	if err := viper.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("error reading config file: %w", err)
	}

	for _, key := range viper.AllKeys() {
		if val, ok := viper.Get(key).(string); ok {
			viper.Set(key, os.ExpandEnv(val))
		}
	}

	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("error unmarshalling config: %w", err)
	}

	return &config, nil
}
