package server

import (
	"context"

	"github.com/gofiber/fiber/v3"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

type Server struct {
	logger *zap.Logger
	app    *fiber.App
}

func NewServer(logger *zap.Logger) *Server {
	app := fiber.New()
	return &Server{
		logger: logger,
		app:    app,
	}
}

func (s *Server) RegisterHandlers() {
	s.logger.Info("Registering HTTP handlers")
	s.app.Get("/", func(c fiber.Ctx) error {
		return c.SendString("Hello, anyserve!")
	})
}

func (s *Server) Start(lifecycle fx.Lifecycle) {
	s.logger.Info("Starting HTTP server", zap.String("addr", ":3000"))

	lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			go func() {
				if err := s.app.Listen(":3000"); err != nil {
					s.logger.Error("Failed to start server", zap.Error(err))
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			s.logger.Info("Stopping HTTP server")
			return s.app.Shutdown()
		},
	})
}

var Module = fx.Provide(NewServer)
