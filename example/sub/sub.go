package main

import (
	"flag"
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
	flag.Parse()
	as := flag.Args()
	var arg string
	if len(as) < 1 {
		arg = ""
	} else {
		arg = as[0]
	}
	switch arg {
	case "dump":
		log.Println("[INFO] Dumping all events.")
		s, err := sub.NewSubscriber(wd, TestEvent{})
		if err != nil {
			log.Fatalf("[FATAL] Cannot create subscriber: %s", err)
		}
		es, err := s.Dump()
		if err != nil {
			log.Fatalf("[FATAL] Cannot dump events: %s", err)
		}
		for _, e := range es {
			log.Printf("[INFO] Event id %s: %#v", e.ID, e.Data)
		}
		os.Exit(0)
	case "":
		log.Println("[INFO] Listening for events, press CTRL-C to stop")
		s, err := sub.NewSubscriber(wd, TestEvent{})
		if err != nil {
			log.Fatalf("[FATAL] Cannot create subscriber: %s", err)
		}
		go func() {
			for {
				select {
				case event := <-s.Queue:
					log.Printf("[INFO] Event %s received: %#v", event.ID, event.Data)
				case <-s.Done:
					return
				}
			}
		}()
		if err := s.Subscribe(); err != nil {
			log.Fatalf("[FATAL] Error listening to events: %s", err)
		}
	default:
		log.Printf("[INFO] Dumping single event: %s", arg)
		s, err := sub.NewSubscriber(wd, TestEvent{})
		if err != nil {
			log.Fatalf("[FATAL] Cannot create subscriber: %s", err)
		}
		e, err := s.Fetch(arg)
		if err != nil {
			log.Fatalf("[FATAL] Cannot dump event: %s", err)
		}
		log.Printf("[INFO] Event id %s: %#v", e.ID, e.Data)
		os.Exit(0)
	}
}
