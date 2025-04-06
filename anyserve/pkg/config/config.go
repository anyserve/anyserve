package config

import (
	"go.uber.org/fx"
)

type HTTPConfig struct {
	Host string
	Port int64
}

func NewHTTPConfig() *HTTPConfig {
	return &HTTPConfig{}
}

type GRPCConfig struct {
	Host       string
	Port       int64
	TLSEnabled bool
	CertFile   string
	KeyFile    string
}

func NewGRPCConfig() *GRPCConfig {
	return &GRPCConfig{}
}

type EmbeddedNATSConfig struct {
	Path string
}

func NewEmbeddedNATSConfig() *EmbeddedNATSConfig {
	return &EmbeddedNATSConfig{}
}

var Module = fx.Provide(NewHTTPConfig, NewGRPCConfig)
