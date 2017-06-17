// Package pub is a very simple event store publisher, designed to send JSON
// events to the file system.
package pub

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"

	"github.com/google/uuid"
)

// IDCollisionError is an error type that is returned on a UUID collision.
// This is a retryable error.
//
// While this error exists and you can check for it, the changes of it
// happening are extremely rare, as the uuid package so explains:
//
//   Randomly generated UUIDs have 122 random bits.  One's annual risk of being
//   hit by a meteorite is estimated to be one chance in 17 billion, that means
//   the probability is about 0.00000000006 (6 × 10−11), equivalent to the odds
//   of creating a few tens of trillions of UUIDs in a year and having one
//   duplicate.
type IDCollisionError struct {
	s string
}

func (e IDCollisionError) Error() string {
	return e.s
}

// Publisher is a simple event publisher, designed to publish events to the
// file system.
type Publisher struct {
	// The directory the event publisher will depost its events in. This is
	// composed of a base directory supplied upon creation of the publisher, and
	// the package-local name of the type used for the event.
	dir string

	// The type for the event that this publisher processes. Events passed to the
	// publisher need to match this type.
	eventType reflect.Type
}

// NewPublisher creates a publisher for the specific type. The events are
// published to a directory composed of the base directory specified in dir,
// and the name of the type.
//
// Any data in event is ignored - it just serves to infer the type of event
// this publisher is locked to.
func NewPublisher(dir string, event interface{}) (*Publisher, error) {
	if event == nil {
		return nil, errors.New("event cannot be nil")
	}
	p := &Publisher{
		dir:       filepath.Clean(dir) + "/" + reflect.TypeOf(event).Name(),
		eventType: reflect.TypeOf(event),
	}

	stat, err := os.Stat(p.dir)
	switch {
	case err == nil:
		if !stat.Mode().IsDir() {
			return nil, fmt.Errorf("%s exists and is not a directory", p.dir)
		}
	case err != nil && os.IsNotExist(err):
		if err := os.Mkdir(p.dir, 0777); err != nil {
			return nil, fmt.Errorf("cannot create directory %s: %s", p.dir, err)
		}
	case err != nil:
		return nil, fmt.Errorf("could not stat dir %s: %s", p.dir, err)
	}

	return p, nil
}

// Publish publishes an event. The event is a single file in the directory,
// with a v4 UUID as the ID and filename. The ID is returned as a string.
func (p *Publisher) Publish(event interface{}) (string, error) {
	if reflect.TypeOf(event) != p.eventType {
		return "", fmt.Errorf("event of type %s does not match publisher type %s", reflect.TypeOf(event), p.eventType)
	}
	data, err := json.Marshal(event)
	if err != nil {
		return "", fmt.Errorf("could not marshal event data: %s", err)
	}
	id, err := uuid.NewRandom()
	if err != nil {
		return "", fmt.Errorf("could not generate ID: %s", err)
	}

	path := p.dir + "/" + id.String()
	// If this path responds to stat, then the path exists in some way, shape, or
	// form, and is not valid for use. This is almost always due to a UUID
	// collision, so return IDCollisionError. If the stat failed and it is due to
	// some other error other than the file being missing, it will be caught when
	// we write to the file.
	if _, err := os.Stat(path); err == nil {
		return "", IDCollisionError{s: fmt.Sprintf("id collision: %s", id)}
	}

	if err := ioutil.WriteFile(path, data, 0666); err != nil {
		return "", fmt.Errorf("error writing event to file %s: %s", path, err)
	}

	return id.String(), nil
}
