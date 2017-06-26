// Package sub provides a very simple event subscriber, using the file system
// as an event store, and the file name for any particular event as the event
// ID. It's designed to be used with the pub package that is also included in
// the fspubsub repository.
package sub

import (
	"fmt"

	"github.com/rjeczalik/notify"
	"github.com/vancluever/fspubsub/store"
)

// defaultNotifyBufferSize describes the default buffer size. This is used for
// both the filesystem and event buffer at this point in time.
//
// This needs to be adequately tuned to the needs of the application - the
// filesystem notifier does not block sending events, so the subscriber will
// miss events if there is an overrun.
const defaultBufferSize = 10

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
	*store.Stream

	// The internal queue channel. Call Queue to get a valid one-way event
	// channel.
	queue chan store.Event

	// The internal completion channel. Call Done to get a valid one-way
	// completion channel.
	done chan struct{}

	// The internal error field. Call Error to receive this externally.
	err error

	// An error channel used to pass errors through and control the subscription
	// lifecycle.
	errch chan error
}

// Queue returns the event channel. This is buffered to the size of the file
// system notification buffer.
func (s *Subscriber) Queue() <-chan store.Event {
	return s.queue
}

// Done returns a channel that closes on termination of the subscription
// goroutine. Block on this to wait until the stream ends.
func (s *Subscriber) Done() <-chan struct{} {
	return s.done
}

// Error returns the subscription goroutine's return status. This is guaranteed
// to be nil if the stream has not yet been terminated, so make sure to block
// on done before checking this value.
func (s *Subscriber) Error() error {
	return s.err
}

// NewSubscriber starts watching the a directory for the event described in
// event. The events are sent over the channel returned by the Queue function.
//
// When the stream shuts down, a message will be sent over the channel returned
// by the Done function to signal that the consumer should stop reading from
// the Queue.
//
// This function returns after the notifier has been set up and the watcher
// goroutine has been successfully started. To wait until the strem has been
// shut down or there has been an error, block on the channel returned by the
// Done function.
//
// The final return status will be contained in the error returned by the Error
// function (nil means no error). If the stream is interrupted for any other
// reason than the subscriber being closed with Close, Error will contain the
// reason for failure.  This includes bad event data, which will shut down the
// subscriber.
func NewSubscriber(dir string, event interface{}) (*Subscriber, error) {
	stream, err := store.NewStream(dir, event)
	if err != nil {
		return nil, err
	}

	s := &Subscriber{
		Stream: stream,
		queue:  make(chan store.Event, defaultBufferSize),
		done:   make(chan struct{}, 1),
		errch:  make(chan error, 1),
	}
	c := make(chan notify.EventInfo, defaultBufferSize)
	if err := notify.Watch(s.Stream.Dir(), c, notify.InCloseWrite); err != nil {
		return nil, fmt.Errorf("error watching directory %s: %s", s.Stream.Dir(), err)
	}
	go s.watch(c)
	return s, nil
}

func (s *Subscriber) watch(c chan notify.EventInfo) {
	defer notify.Stop(c)
	for {
		select {
		case ei := <-c:
			e, err := store.DecodeEvent(ei.Path(), s.Stream.EventType())
			if err != nil {
				s.errch <- err
				break
			}
			s.queue <- e
		case s.err = <-s.errch:
			close(s.done)
			return
		}
	}
}

// NewSubscriberWithCallback is a helper that provides a very simple event loop
// around a Subscriber. Events are passed to the callback function supplied by
// cb.
//
// This function does not take responsbility for handling event processing
// errors. The caller should ensure they are handling errors as normal by
// waiting on the Done channel and processing any errors returned by the Error
// function afterward.
func NewSubscriberWithCallback(dir string, event interface{}, cb func(store.Event)) (*Subscriber, error) {
	s, err := NewSubscriber(dir, event)
	if err != nil {
		return nil, err
	}
	go func() {
		for {
			select {
			case event := <-s.Queue():
				cb(event)
			case <-s.Done():
				return
			}
		}
	}()
	return s, nil
}

// Close signals to the Subscriber that we are done and that the subscription
// is no longer needed. This performs a graceful shutdown of the subscriber.
func (s *Subscriber) Close() {
	close(s.errch)
}
