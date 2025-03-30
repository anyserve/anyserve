package server

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/anyserve/anyserve/pkg/config"
	"github.com/gofiber/contrib/fiberzap"
	"github.com/gofiber/fiber/v2"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

type Server struct {
	logger *zap.Logger
	config *config.Config
	app    *fiber.App
}

func NewServer(logger *zap.Logger, cfg *config.Config) *Server {
	app := fiber.New(fiber.Config{
		JSONEncoder: json.Marshal,
		JSONDecoder: json.Unmarshal,
		Prefork:     false,
	})
	return &Server{
		logger: logger,
		app:    app,
		config: cfg,
	}
}

func (s *Server) RegisterHandlers() {
	s.logger.Info("Registering HTTP handlers")
	s.app.Use(fiberzap.New(fiberzap.Config{
		SkipBody:    func(c *fiber.Ctx) bool { return false },
		SkipResBody: func(c *fiber.Ctx) bool { return false },
		Logger:      s.logger,
		Fields:      s.config.Server.Logger.Fields,
	}))
	s.app.Get("/", func(c *fiber.Ctx) error {
		return c.SendString("Hello, anyserve!")
	})
	s.app.Get("/healthz", healthz)
}

func (s *Server) Start(lifecycle fx.Lifecycle) {
	addr := fmt.Sprintf("%s:%d", s.config.Server.Host, s.config.Server.Port)
	s.logger.Info("Starting HTTP server", zap.String("addr", addr))

	lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			go func() {
				if err := s.app.Listen(addr); err != nil {
					s.logger.Error("Failed to start server", zap.Error(err))
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			s.logger.Info("Stopping HTTP server")
			return s.app.ShutdownWithContext(ctx)
		},
	})
}

var Module = fx.Provide(NewServer)
