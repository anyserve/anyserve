package server

import (
	"github.com/gofiber/contrib/fiberzap"
	"github.com/gofiber/fiber/v2"
)

func (s *Server) zapLogger() fiber.Handler {
	return fiberzap.New(fiberzap.Config{
		SkipBody:    func(c *fiber.Ctx) bool { return false },
		SkipResBody: func(c *fiber.Ctx) bool { return false },
		Logger:      s.logger,
		Fields:      s.config.Server.Logger.Fields,
		Messages:    []string{"Failed", "Denied", "OK"},
	})
}
