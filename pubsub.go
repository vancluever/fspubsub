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
// To listen for events on the stream, create a new subscriber, and then call
// Subscribe. This will start to send events through the respective channel.
//
//   s, err := sub.NewSubscriber(wd, TestEvent{})
//   if err != nil {
//     log.Fatalf("[FATAL] Cannot create subscriber: %s", err)
//   }
//   go func() {
//     for {
//       select {
//       case event := <-s.Queue:
//         log.Printf("[INFO] Event %s received: %#v", event.ID, event.Data)
//       case <-s.Done:
//         return
//       }
//     }
//   }()
//   if err := s.Subscribe(); err != nil {
//     log.Fatalf("[FATAL] Error listening to events: %s", err)
//   }
//
// Subscribe blocks until there is an error in the stream or the subscription is
// shut down with Close.
package fspubsub
