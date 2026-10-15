// Package echo provides an adapter for integrating blazesse with the Echo web framework.
package echo

import (
	"github.com/labstack/echo/v4"

	"github.com/rlsvr/blazesse"
)

// Upgrade upgrades an Echo request context to an active 1-to-1 Server-Sent Events stream.
func Upgrade(c echo.Context, opts ...blazesse.Option) (*blazesse.Stream, error) {
	return blazesse.Upgrade(c.Response(), c.Request(), opts...)
}

// Handler returns an Echo handler func that mounts a blazesse Broadcaster directly.
func Handler(b *blazesse.Broadcaster) echo.HandlerFunc {
	return func(c echo.Context) error {
		b.ServeHTTP(c.Response(), c.Request())
		return nil
	}
}
