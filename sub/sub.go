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
	"sort"

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
	// The internal queue channel. Call Queue to get a valid one-way event
	// channel.
	queue chan Event

	// The internal completion channel. Call Done to get a valid one-way
	// completion channel.
	done chan struct{}

	// The internal error field. Call Error to receive this externally.
	err error

	// An error channel used to pass errors through and control the subscription
	// lifecycle.
	errch chan error

	// The directory the event publisher will read events from. This is composed
	// of a base directory supplied upon creation of the publisher, and the
	// package-local name of the type used for the event.
	dir string

	// The type for the event that this publisher processes. Events passed to the
	// publisher need to match this type.
	eventType reflect.Type

	// If this is true, this subscriber has already been used for watching an
	// event stream and cannot be used again.
	opened bool
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
		queue:     make(chan Event, defaultBufferSize),
		done:      make(chan struct{}, 1),
		dir:       filepath.Clean(dir) + "/" + reflect.TypeOf(event).Name(),
		eventType: reflect.TypeOf(event),
		errch:     make(chan error, 1),
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

// Queue returns the event channel. This is buffered to the size of the file
// system notification buffer.
func (s *Subscriber) Queue() <-chan Event {
	return s.queue
}

// Done returns a channel that closes on termination of the subscription
// goroutine. Block on this to wait until the stream ends.
func (s *Subscriber) Done() <-chan struct{} {
	return s.done
}

// Err returns the subscription goroutine's return status. This is guaranteed
// to be nil if the stream has not yet been terminated, so make sure to block
// on done before checking this value.
func (s *Subscriber) Error() error {
	return s.err
}

// Subscribe starts watching the directory for events, and sends the events
// over the Queue channel.
//
// Only one call to Subscribe can ever be made. A second call results in a
// panic. If you have used a subscriber to watch a stream and it has closed or
// errored out, create a new Subscriber.
//
// When the stream shuts down, a message will be sent over the Done channel to
// signal that the consumer should stop reading from the Queue.
//
// This function returns after the notifier has been set up and the watcher
// goroutine has been successfully started. To block until the strem has been
// shut down or there has been an error, block on the Done channel. The error
// will then be in the Err attribute, nil or not.
//
// If the stream is interrupted for any other reason than the subscriber being
// closed with Close, Err will contain an error, otherwise it will be nil. This
// includes bad event data, which will shut down the subscriber.
func (s *Subscriber) Subscribe() error {
	if s.opened {
		panic("calling subscribe on an already opened subscriber")
	}
	c := make(chan notify.EventInfo, defaultBufferSize)
	if err := notify.Watch(s.dir, c, notify.InCloseWrite); err != nil {
		return fmt.Errorf("error watching directory %s: %s", s.dir, err)
	}
	s.opened = true
	go s.watch(c)
	return nil
}

func (s *Subscriber) watch(c chan notify.EventInfo) {
	defer notify.Stop(c)
	for {
		select {
		case ei := <-c:
			d := reflect.New(s.eventType)
			b, err := ioutil.ReadFile(ei.Path())
			if err != nil {
				s.errch <- fmt.Errorf("error reading event data at %s: %s", ei.Path(), err)
				break
			}
			if err := json.Unmarshal(b, d.Interface()); err != nil {
				s.errch <- fmt.Errorf("error unmarshaling event data from %s: %s", ei.Path(), err)
				break
			}
			s.queue <- Event{
				ID:   filepath.Base(ei.Path()),
				Data: d.Elem().Interface(),
			}
		case s.err = <-s.errch:
			close(s.done)
			return
		}
	}
}

// SubscribeCallback is a helper that provides a very simple event loop around
// Subscribe. Events are passed to the callback function supplied by cb, with
// the ID and data.
//
// This function does not take responsbility for handling event processing
// errors. It's up to the callback to handle errors as it sees fit.
func (s *Subscriber) SubscribeCallback(cb func(string, interface{})) error {
	go func() {
		for {
			select {
			case event := <-s.Queue():
				cb(event.ID, event.Data)
			case <-s.Done():
				return
			}
		}
	}()
	return s.Subscribe()
}

// Close signals to the Subscriber that we are done and that the subscription
// is no longer needed. This performs a graceful shutdown of the subscriber.
func (s *Subscriber) Close() {
	close(s.errch)
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

// DumpSorted dumps all of the events, and then sorts them according to the
// criteria defined by the event type's IndexedEvent interface. The function
// will panic during sort if this interface is not implemented.
func (s *Subscriber) DumpSorted() ([]Event, error) {
	es, err := s.Dump()
	if err != nil {
		return nil, err
	}
	sort.Sort(eventSlice(es))
	return es, nil
}

// DumpSortedReverse acts as per DumpSorted, but reverses the sort order,
// normally giving a descending order rather than an ascending one.
func (s *Subscriber) DumpSortedReverse() ([]Event, error) {
	es, err := s.Dump()
	if err != nil {
		return nil, err
	}
	sort.Sort(sort.Reverse(eventSlice(es)))
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
