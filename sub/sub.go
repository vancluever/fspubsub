// Package sub provides a very simple event subscriber, using the file system
// as an event store, and the file name for any particular event as the event
// ID. It's designed to be used with the pub package that is also included in
// the fspubsub repository.
package sub

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"sync"

	"github.com/rjeczalik/notify"
)

// defaultNotifyBufferSize describes the default buffer size. This is used for
// both the filesystem and event buffer at this point in time.
//
// This needs to be adequately tuned to the needs of the application - the
// filesystem notifier does not block sending events, so the subscriber will
// miss events if there is an overrun.
const defaultBufferSize = 10

// Event represents a single event.
type Event struct {
	// The ID of the event. This normally translates to the file name from the
	// store.
	ID string

	// The event data.
	Data interface{}
}

// Subscriber is a simple event subscriber, designed to read events from the
// file system.
//
// The stream location on the filesystem is composed of a base directory and
// the name of the type that you are watching, without the package name
// included. As an example, if you set the directory to be ./, and the type you
// were watching was main.TestEvent, the stream path would be ./TestEvent. The
// directory is created if it does not exist.
//
// Note that the directory the event store is in must only contain events -
// functions will fail if they encounter non-event data (ie: JSON that it
// cannot parse into the event type).
type Subscriber struct {
	// The event channel. This is buffered to the size of the file system
	// notification buffer.
	Queue chan Event

	// The done channel. This should be watched to determine if the event stream
	// has been shut down.
	Done chan struct{}

	// The directory the event publisher will read events from. This is composed
	// of a base directory supplied upon creation of the publisher, and the
	// package-local name of the type used for the event.
	dir string

	// The type for the event that this publisher processes. Events passed to the
	// publisher need to match this type.
	eventType reflect.Type

	// A mutex for blocking access to the watcher.
	m sync.Mutex

	// An internal channel for signaling that we are done watching FS events.
	fsDone chan struct{}
}

// NewSubscriber creates a subscriber to a directory-based event stream, being
// a mix of the path supplied and the event type passed to event.
//
// Any data in event is ignored - it just serves to infer the type of event
// this subscriber is locked to.
func NewSubscriber(dir string, event interface{}) (*Subscriber, error) {
	if event == nil {
		return nil, errors.New("event cannot be nil")
	}
	s := &Subscriber{
		Queue:     make(chan Event, defaultBufferSize),
		Done:      make(chan struct{}, 1),
		dir:       filepath.Clean(dir) + "/" + reflect.TypeOf(event).Name(),
		eventType: reflect.TypeOf(event),
		fsDone:    make(chan struct{}, 1),
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
		return nil, fmt.Errorf("Could not stat dir %s: %s", s.dir, err)
	}

	return s, nil
}

// Subscribe starts watching the directory for events, and sends the events
// over the Queue channel. Only one subscription can be open at any point in
// time - this function blocks if the subscriber is currently open.
//
// When the stream shuts down, a message will be sent over the Done channel to
// signal that the consumer should stop reading from the Queue.
//
// If the stream is interrupted for any other reason than the subscriber being
// closed with Close, this function will return an error. This includes bad
// event data, which will shut down the subscriber.
func (s *Subscriber) Subscribe() error {
	s.m.Lock()
	defer s.m.Unlock()
	c := make(chan notify.EventInfo, defaultBufferSize)
	if err := notify.Watch(s.dir, c, notify.InCloseWrite); err != nil {
		return fmt.Errorf("error watching directory %s: %s", s.dir, err)
	}
	defer notify.Stop(c)
	for {
		select {
		case ei := <-c:
			d := reflect.New(s.eventType)
			b, err := ioutil.ReadFile(ei.Path())
			if err != nil {
				return fmt.Errorf("error reading event data at %s: %s", ei.Path(), err)
			}
			if err := json.Unmarshal(b, d.Interface()); err != nil {
				return fmt.Errorf("error unmarshaling event data from %s: %s", ei.Path(), err)
			}
			s.Queue <- Event{
				ID:   filepath.Base(ei.Path()),
				Data: d.Elem().Interface(),
			}
		case <-s.fsDone:
			goto done
		}
	}
done:
	s.Done <- struct{}{}
	return nil
}

// SubscribeCallback is a helper that provides a very simple event loop around
// Subscribe. Events are passed to the callback function supplied by cb, with
// the ID and data.
//
// This function does not take responsbility for handling event processing
// errors. It's up to the callback to hand errors as it sees fit.
func (s *Subscriber) SubscribeCallback(cb func(string, interface{})) error {
	go func() {
		for {
			select {
			case event := <-s.Queue:
				cb(event.ID, event.Data)
			case <-s.Done:
				return
			}
		}
	}()
	return s.Subscribe()
}

// Close signals to Subscribe that we are done and that the subscription is no
// longer needed. This initiates shutdown in Subscribe and will make it return
// without error, as long as there is none.
func (s *Subscriber) Close() {
	s.fsDone <- struct{}{}
}

// Dump dumps all of the events in the store for this stream. Technically, it's
// just dumping all of the events in the directory that the stream has been
// configured to watch. This is returned as an Event slice.
//
// The order of the returned events is not deterministic. If it is up to the
// consumer to structure the data or the handling of the data in a way that
// facilitates proper hydration.
func (s *Subscriber) Dump() ([]Event, error) {
	entries, err := ioutil.ReadDir(s.dir)
	var es []Event
	if err != nil {
		return nil, fmt.Errorf("error reading event directory %s: %s", s.dir, err)
	}
	for _, f := range entries {
		if !f.Mode().IsRegular() {
			continue
		}
		id := f.Name()
		ep := s.dir + "/" + id
		d := reflect.New(s.eventType)
		b, err := ioutil.ReadFile(ep)
		if err != nil {
			return nil, fmt.Errorf("error reading event data at %s: %s", ep, err)
		}
		if err := json.Unmarshal(b, d.Interface()); err != nil {
			return nil, fmt.Errorf("error unmarshaling event data from %s: %s", ep, err)
		}
		es = append(es, Event{
			ID:   id,
			Data: d.Elem().Interface(),
		})
	}
	return es, nil
}

// Fetch reads a single event from the store. The supplied ID is essentially the
// file name.
func (s *Subscriber) Fetch(id string) (Event, error) {
	var e Event
	ep := s.dir + "/" + id
	d := reflect.New(s.eventType)
	b, err := ioutil.ReadFile(ep)
	if err != nil {
		return e, fmt.Errorf("error reading event data at %s: %s", ep, err)
	}
	if err := json.Unmarshal(b, d.Interface()); err != nil {
		return e, fmt.Errorf("error unmarshaling event data from %s: %s", ep, err)
	}
	e.ID = id
	e.Data = d.Elem().Interface()
	return e, nil
}
