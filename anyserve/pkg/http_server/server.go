package http_server

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/anyserve/anyserve/pkg/config"
	"github.com/anyserve/anyserve/pkg/utils"
	"github.com/gofiber/fiber/v2"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

var logger = utils.GetLogger("server")

type Server struct {
	config *config.HTTPConfig

	app *fiber.App
}

func NewServer(lc fx.Lifecycle, cfg *config.HTTPConfig) *Server {
	app := fiber.New(fiber.Config{
		JSONEncoder: json.Marshal,
		JSONDecoder: json.Unmarshal,
		Prefork:     false,
	})

	s := &Server{
		app:    app,
		config: cfg,
	}

	s.app.Use(zapLogger())
	s.app.Get("/healthz", healthz)

	s.app.Get("/", func(c *fiber.Ctx) error {
		return c.SendString("Hello, anyserve!")
	})

	addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			go func() {
				if err := s.app.Listen(addr); err != nil {
					logger.Error("Failed to start server", zap.Error(err))
				}
			}()
			logger.Sugar().Infof("HTTP server started on %s", addr)
			return nil
		},
		OnStop: func(ctx context.Context) error {
			logger.Sugar().Info("Stopping HTTP server")
			return s.app.ShutdownWithContext(ctx)
		},
	})
	return s
}
