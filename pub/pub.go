// Package pub is a very simple event store publisher, designed to send JSON
// events to the file system.
package pub

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/vancluever/fspubsub/store"
)

// Publisher is a simple event publisher, designed to publish events to the
// file system.
type Publisher struct {
	*store.Stream
}

// NewPublisher creates a publisher for the specific type. The events are
// published to a directory composed of the base directory specified in dir,
// and the name of the type.
//
// Any data in event is ignored - it just serves to infer the type of event
// this publisher is locked to.
func NewPublisher(dir string, event interface{}) (*Publisher, error) {
	stream, err := store.NewStream(dir, event)
	if err != nil {
		return nil, err
	}
	p := &Publisher{
		Stream: stream,
	}
	return p, nil
}

// Publish publishes an event. The event is a single file in the directory,
// with a v4 UUID as the ID and filename. The ID is returned as a string.
func (p *Publisher) Publish(event interface{}) (string, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return "", fmt.Errorf("could not generate ID: %s", err)
	}

	if err := p.Stream.WriteEvent(id.String(), event); err != nil {
		return "", err
	}

	return id.String(), nil
}
