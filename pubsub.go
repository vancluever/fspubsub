// Package fspubsub is a very simple streaming event store, good to demonstrate the
// basics of event publishing and subscription. It publishes events directly to the
// filesystem, with the subscriber listening to the publishing location with
// inotify. It also has facilities to pull the entire current store, and a single
// event.
//
// Note that the hook on inotify events currently means that this package is
// supported on Linux only.
//
// Usage Synopsis
//
// Streams are named for the type of event they support. An event is a type,
// although realistically, it is probably a struct:
//
//   type TestEvent struct {
//     Text string
//   }
//
// Using the above type, you can start a stream by creating a publisher for the
// event you will be publishing to:
//
//   p, err := pub.NewPublisher("./", TestEvent{})
//   if err != nil {
//     return log.Fatalf("[FATAL] Could not create stream: %s", err)
//   }
//
// Note that the struct that you pass to the event in NewPublisher does not need
// to have data in it, and in fact is ignored if there is any.
//
// After the publisher is created, sending the event is as easy as sending the
// struct along:
//
//   e := &TestEvent{Text: "Foo"}
//   id, err := p.Publish(e)
//   if err != nil {
//     return log.Fatalf("[FATAL] Could not publish event: %s", err)
//   }
//
// To listen for events on the stream, create a new subscriber via NewSubscriber.
// You can then use Queue() to get a channel where you can watch for events, and
// Done() to get a channel that will close when the stream is done or fails for
// some other reason. The error will be in Error() when done, if any.
//
//   s, err := sub.NewSubscriber(wd, TestEvent{})
//   if err != nil {
//     log.Fatalf("[FATAL] Cannot create subscriber: %s", err)
//   }
//   go func() {
//     for {
//       select {
//       case event := <-s.Queue():
//         // Do something with event here
//       case <-s.Done():
//         return
//       }
//     }
//   }()
//   <-s.Done()
//   if s.Error() != nil {
//     log.Fatalf("[FATAL] Error while listening to events: %s", s.Error())
//   }
//
// If all you need in your event loop is this basic setup, you can also use
// NewSubscriberWithCallback:
//
//   cb := func(e sub.Event) {
//         log.Printf("[INFO] Event %s received: %#v", e.ID, e.Data)
//   }
//   s, err := sub.NewSubscriberWithCallback(wd, TestEvent{}, cb)
//   if err != nil {
//     log.Fatalf("[FATAL] Cannot create subscriber: %s", err)
//   }
//   <-s.Done()
//   if s.Error() != nil {
//     log.Fatalf("[FATAL] Error while listening to events: %s", s.Error())
//   }
//
package fspubsub
