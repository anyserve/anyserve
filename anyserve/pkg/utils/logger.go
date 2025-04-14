package utils

import (
	"os"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var logLevel = zap.NewAtomicLevel()
var logFormat = "console" // 默认使用console格式

var mu sync.Mutex
var loggers = make(map[string]*logHandle)

type logHandle struct {
	*zap.Logger
	name string
	pid  int
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
	pid := os.Getpid()
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.RFC3339NanoTimeEncoder
	logConfig := zap.Config{
		Level:             logLevel,
		Development:       false,
		DisableCaller:     false,
		DisableStacktrace: false,
		Sampling:          nil,
		Encoding:          logFormat,
		EncoderConfig:     encoderConfig,
		OutputPaths:       []string{"stdout"},
		ErrorOutputPaths:  []string{"stderr"},
		InitialFields: map[string]interface{}{
			"name": name,
			"pid":  pid,
		},
	}

	logger, _ := logConfig.Build()

	l := &logHandle{Logger: logger, name: name, pid: pid}
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

// SetFormat 设置日志格式为json或console
func SetFormat(format string) {
	mu.Lock()
	defer mu.Unlock()

	switch format {
	case "json", "console":
		logFormat = format
	default:
		logFormat = "console"
	}

	// 清除并重新创建所有logger
	oldLoggers := loggers
	loggers = make(map[string]*logHandle)

	// 为每个旧的logger创建新的实例
	for name := range oldLoggers {
		loggers[name] = newLogger(name)
	}
}
