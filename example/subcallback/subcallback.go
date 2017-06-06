package main

import (
	"log"
	"os"

	"github.com/vancluever/fspubsub/sub"
)

type TestEvent struct {
	Text string
}

func main() {
	wd, err := os.Getwd()
	if err != nil {
		log.Fatalf("cannot get working directory: %s", err)
	}
	s, err := sub.NewSubscriber(wd, TestEvent{})
	if err != nil {
		log.Fatalf("[FATAL] Cannot create subscriber: %s", err)
	}
	log.Println("[INFO] Listening for events, press CTRL-C to stop")
	cb := func(id string, data interface{}) {
		log.Printf("[INFO] Event %s received: %#v", id, data)
	}
	if err := s.SubscribeCallback(cb); err != nil {
		log.Fatalf("[FATAL] Error listening to events: %s", err)
	}
}
