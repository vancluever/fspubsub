// Package store provides utility functionality to the pub and sub packages,
// and also includes functions for fetching, dumping, and generally working
// with events.
package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"sort"
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

// Event represents a single event.
type Event struct {
	// The ID of the event. This normally translates to the file name from the
	// store.
	ID string

	// The event data.
	Data interface{}
}

// eventSlice represents multiple events and implements sort.Interface so that
// the slice can be sorted by the event struct's implementation of
// IndexedEvent, if it exists. Data sorted by this function gets returned as
// just the generic event slice.
type eventSlice []Event

// Len returns the length of the slice, and helps implement sort.Interface.
func (p eventSlice) Len() int {
	return len(p)
}

// Less takes the value at the index of j, and passes it to the Less method
// of the event data at the index of i. The function panics if p[j] does not
// implement IndexedEvent.
func (p eventSlice) Less(i, j int) bool {
	v, ok := p[i].Data.(IndexedEvent)
	if !ok {
		panic(fmt.Errorf("Event at index %d with type %T does not implement sub.IndexedEvent", i, p[i]))
	}
	return v.Less(p[j].Data)
}

// Swap does a simple swap of i and j, implementing sort.Interface.
func (p eventSlice) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

// IndexedEvent is an interface that implements an event with an index.
//
// The index, whatever is chosen in the event, should be able to be sorted with
// a Less method. This method is similar in purpose to the Less method in the
// standard library sort package, however, the lower index value commonly
// denoted as i should be a receiver. A trivial example is below:
//
//   type E struct {
//     Index string
//   }
//
//   func (i E) Less(j interface{}) bool {
//     return i.Index < j.(E).Index
//   }
//
type IndexedEvent interface {
	Less(j interface{}) bool
}

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

// WriteEvent writes an event, with the file name taking on the ID passed in to
// id. This is generally designed to be used by publishers in the pub package,
// but is separated to help with testing.
func (s *Stream) WriteEvent(id string, event interface{}) error {
	if reflect.TypeOf(event) != s.EventType() {
		return fmt.Errorf("event of type %s does not match stream type %s", reflect.TypeOf(event), s.EventType())
	}
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("could not marshal event data: %s", err)
	}

	path := s.Dir() + "/" + id
	// If this path responds to stat, then the path exists in some way, shape, or
	// form, and is not valid for use. This is almost always due to a UUID
	// collision, so return IDCollisionError. If the stat failed and it is due to
	// some other error other than the file being missing, it will be caught when
	// we write to the file.
	if _, err := os.Stat(path); err == nil {
		return IDCollisionError{s: fmt.Sprintf("id collision: %s", id)}
	}

	if err := ioutil.WriteFile(path, data, 0666); err != nil {
		return fmt.Errorf("error writing event to file %s: %s", path, err)
	}

	return nil
}

// Dump dumps all of the events in the store for stream described by dir and
// event. Technically, it's just dumping all of the events in the directory.
// The events are returned as an Event slice.
//
// The order of the returned events is not deterministic. If it is up to the
// consumer to structure the data or the handling of the data in a way that
// facilitates proper hydration.
func Dump(dir string, event interface{}) ([]Event, error) {
	stream, err := NewStream(dir, event)
	if err != nil {
		return nil, err
	}

	return dump(stream)
}

func dump(s *Stream) ([]Event, error) {
	entries, err := ioutil.ReadDir(s.Dir())
	var es []Event
	if err != nil {
		return nil, fmt.Errorf("error reading event directory %s: %s", s.Dir(), err)
	}
	for _, f := range entries {
		if !f.Mode().IsRegular() {
			continue
		}
		e, err := DecodeEvent(s.Dir()+"/"+f.Name(), s.EventType())
		if err != nil {
			return nil, err
		}
		es = append(es, e)
	}
	return es, nil
}

// DecodeEvent is an internal helper that decodes a file at path and returns an
// Event.
func DecodeEvent(path string, eventType reflect.Type) (Event, error) {
	d := reflect.New(eventType)
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return Event{}, fmt.Errorf("error reading event data at %s: %s", path, err)
	}
	if err := json.Unmarshal(b, d.Interface()); err != nil {
		return Event{}, fmt.Errorf("error unmarshaling event data from %s: %s", path, err)
	}
	return Event{
		ID:   filepath.Base(path),
		Data: d.Elem().Interface(),
	}, nil
}

// DumpSorted works as per Dump, but sorts the returned events according to the
// criteria defined by the event type's IndexedEvent interface. The function
// will panic during sort if this interface is not implemented.
func DumpSorted(dir string, event interface{}) ([]Event, error) {
	es, err := Dump(dir, event)
	if err != nil {
		return nil, err
	}
	sort.Sort(eventSlice(es))
	return es, nil
}

// DumpSortedReverse acts as per DumpSorted, but reverses the sort order,
// normally giving a descending order rather than an ascending one.
func DumpSortedReverse(dir string, event interface{}) ([]Event, error) {
	es, err := Dump(dir, event)
	if err != nil {
		return nil, err
	}
	sort.Sort(sort.Reverse(eventSlice(es)))
	return es, nil
}

// Fetch reads a single event from the store described by dir and type. The
// supplied ID is essentially the file name.
func Fetch(dir string, event interface{}, id string) (Event, error) {
	stream, err := NewStream(dir, event)
	if err != nil {
		return Event{}, err
	}
	return DecodeEvent(stream.Dir()+"/"+id, stream.EventType())
}
