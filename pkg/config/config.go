package config

import (
	"fmt"
	"strings"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/confmap"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
	"go.uber.org/fx"
)

type Config struct {
	Server ServerConfig `koanf:"server"`
	Logger LoggerConfig `koanf:"logger"`
}

type ServerConfig struct {
	Host   string             `koanf:"host"`
	Port   int                `koanf:"port"`
	Logger ServerLoggerConfig `koanf:"logger"`
}

func (c *ServerConfig) Validate() error {
	if c.Port <= 0 || c.Port > 65535 {
		return fmt.Errorf("invalid server port: %d", c.Port)
	}
	return nil
}

type ServerLoggerConfig struct {
	Fields []string `koanf:"fields"`
}

type LoggerConfig struct {
	Level       string `koanf:"level"`
	Development bool   `koanf:"development"`
}

func defaultConfig() map[string]interface{} {
	defaultConfig := map[string]interface{}{
		"server.host":          "0.0.0.0",
		"server.port":          8848,
		"server.logger.fields": []string{"method", "path", "status"},
		"logger.level":         "info",
		"logger.development":   false,
	}
	return defaultConfig
}

// Load config order:
// 1. default config
// 2. config.yaml
// 3. environment variables (starts with ANYSERVE_)
func NewConfig(filePath string) (*Config, error) {
	k := koanf.New(".")

	if err := k.Load(confmap.Provider(defaultConfig(), "."), nil); err != nil {
		return nil, fmt.Errorf("error loading default config: %w", err)
	}

	if err := k.Load(file.Provider(filePath), yaml.Parser()); err != nil {
		return nil, fmt.Errorf("error loading config: %w", err)
	}

	if err := k.Load(env.Provider("ANYSERVE_", ".", func(s string) string {
		return strings.Replace(strings.ToLower(
			strings.TrimPrefix(s, "ANYSERVE_")), "_", ".", -1)
	}), nil); err != nil {
		return nil, fmt.Errorf("error loading environment variables: %w", err)
	}

	var cfg Config
	if err := k.Unmarshal("", &cfg); err != nil {
		return nil, fmt.Errorf("error unmarshalling config: %w", err)
	}

	return &cfg, nil
}

func (c *Config) Validate() error {
	if err := c.Server.Validate(); err != nil {
		return fmt.Errorf("invalid server config: %w", err)
	}
	return nil
}

var Module = fx.Provide(NewConfig)
