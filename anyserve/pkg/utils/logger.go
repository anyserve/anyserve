package utils

import (
	"os"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var logLevel = zap.NewAtomicLevel()

var mu sync.Mutex
var loggers = make(map[string]*logHandle)

type logHandle struct {
	*zap.Logger

	name  string
	level zapcore.Level
	pid   int
}

func GetLogger(name string) *logHandle {
	mu.Lock()
	defer mu.Unlock()

	if logger, ok := loggers[name]; ok {
		return logger
	}
	logger := newLogger(name)
	loggers[name] = logger
	return logger
}

func newLogger(name string) *logHandle {
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.EpochNanosTimeEncoder
	logConfig := zap.Config{
		Level:             logLevel,
		Development:       false,
		DisableCaller:     false,
		DisableStacktrace: false,
		Sampling:          nil,
		Encoding:          "json",
		EncoderConfig:     encoderConfig,
		OutputPaths:       []string{"stdout"},
		ErrorOutputPaths:  []string{"stderr"},
	}

	logger, _ := logConfig.Build()

	l := &logHandle{Logger: logger, name: name, pid: os.Getpid()}
	return l
}

func SetLevel(level string) {
	var zlevel zapcore.Level
	switch level {
	case "debug":
		zlevel = zapcore.DebugLevel
	case "info":
		zlevel = zapcore.InfoLevel
	case "warn":
		zlevel = zapcore.WarnLevel
	case "error":
		zlevel = zapcore.ErrorLevel
	default:
		zlevel = zapcore.InfoLevel
	}
	logLevel.SetLevel(zlevel)
}
