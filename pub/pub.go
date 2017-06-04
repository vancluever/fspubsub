// Package pub is a very simple event store publisher, designed to send JSON
// events to the file system.
package pub

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
)

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
		return nil, fmt.Errorf("Could not stat dir %s: %s", p.dir, err)
	}

	return p, nil
}

// Publish publishes an event. The event is a single file in the directory,
// with a random 32-byte hex as the ID and filename. The ID is returned as a
// string.
func (p *Publisher) Publish(event interface{}) (string, error) {
	if reflect.TypeOf(event) != p.eventType {
		return "", fmt.Errorf("event of type %s does not match publisher type %s", reflect.TypeOf(event), p.eventType)
	}
	data, err := json.Marshal(event)
	if err != nil {
		return "", fmt.Errorf("could not marshal event data: %s", err)
	}
	idb := make([]byte, 32)
	if _, err := rand.Read(idb); err != nil {
		return "", fmt.Errorf("could not generate ID: %s", err)
	}
	id := fmt.Sprintf("%x", idb)

	path := p.dir + "/" + id

	if err := ioutil.WriteFile(path, data, 0666); err != nil {
		return "", fmt.Errorf("error writing event to file %s: %s", path, err)
	}

	return id, nil
}
