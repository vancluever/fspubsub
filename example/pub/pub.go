package main

import (
	"flag"
	"log"
	"os"
	"strings"

	"github.com/vancluever/fspubsub/pub"
)

type TestEvent struct {
	Text string
}

var event TestEvent

func main() {
	flag.Parse()
	event.Text = strings.Join(flag.Args(), " ")
	log.Printf("[INFO] Publishing event: %#v", event)
	wd, err := os.Getwd()
	if err != nil {
		log.Fatalf("[FATAL] Cannot get working directory: %s", err)
	}
	p, err := pub.NewPublisher(wd, event)
	if err != nil {
		log.Fatalf("[FATAL] Cannot create publisher: %s", err)
	}
	id, err := p.Publish(event)
	if err != nil {
		log.Fatalf("[FATAL] Error publish error: %s", err)
	}
	log.Printf("[INFO] Event published successfully: %s", id)
}
