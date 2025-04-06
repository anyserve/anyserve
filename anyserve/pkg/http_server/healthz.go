package http_server

import "github.com/gofiber/fiber/v2"

func healthz(c *fiber.Ctx) error {
	return c.SendString("ok")
}
