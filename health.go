package nats

import (
	"fmt"

	"github.com/nats-io/nats.go"
)

// ConnectionHealth checks connection health.
func (p *Producer) ConnectionHealth() error {
	if status := p.js.Conn().Status(); status != nats.CONNECTED {
		return fmt.Errorf("NATS connection status is %s", status.String()) //nolint:err113
	}
	return nil
}

// ConnectionHealth checks connection health.
func (c *Consumer) ConnectionHealth() error {
	if status := c.connection.Status(); status != nats.CONNECTED {
		return fmt.Errorf("NATS connection status is %s", status.String()) //nolint:err113
	}
	return nil
}
