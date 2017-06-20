// Package store provides utility functionality to the pub and sub packages.
// It's probably not useful on its own, but is exported out of necessity.
package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
)

// Stream describes a directory-based event stream. It is a utility object used
// by the pub and sub packages, mostly to set up a directory for a specific
// event.
type Stream struct {
	// The directory the stream will work in. This is composed of a base
	// directory supplied upon creation, and the package-local name of the type
	// used for the event.
	dir string

	// The type for the event that this stream processes. Events passed to the
	// stream should match this type.
	eventType reflect.Type
}

// NewStream creates a stream for the specific type. The events are read or
// written to a directory composed of the base directory specified in dir, and
// the name of the type.
//
// Any data in event is ignored - it just serves to infer the type of event
// this publisher is locked to.
func NewStream(dir string, event interface{}) (*Stream, error) {
	if event == nil {
		return nil, errors.New("event cannot be nil")
	}
	s := &Stream{
		dir:       filepath.Clean(dir) + "/" + reflect.TypeOf(event).Name(),
		eventType: reflect.TypeOf(event),
	}

	stat, err := os.Stat(s.dir)
	switch {
	case err == nil:
		if !stat.Mode().IsDir() {
			return nil, fmt.Errorf("%s exists and is not a directory", s.dir)
		}
	case err != nil && os.IsNotExist(err):
		if err := os.Mkdir(s.dir, 0777); err != nil {
			return nil, fmt.Errorf("cannot create directory %s: %s", s.dir, err)
		}
	case err != nil:
		return nil, fmt.Errorf("could not stat dir %s: %s", s.dir, err)
	}

	return s, nil
}

// Dir returns the full path for the event store.
func (s *Stream) Dir() string {
	return s.dir
}

// EventType returns the event type for the stream.
func (s *Stream) EventType() reflect.Type {
	return s.eventType
}
