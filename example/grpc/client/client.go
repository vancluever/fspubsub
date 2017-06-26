package main

import (
	"context"
	"io"
	"log"
	"os"
	"os/signal"

	"github.com/vancluever/fspubsub/example/grpc/events"
	"google.golang.org/grpc"
)

func main() {
	conn, err := grpc.Dial("localhost:8080", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("[FATAL] Could not connect on localhost:8080: %s", err)
	}
	defer conn.Close()
	client := events.NewEventSubscriberClient(conn)
	stream, err := client.Subscribe(context.Background())
	if err != nil {
		log.Fatalf("[FATAL] Could not subscribe to stream: %s", err)
	}
	log.Printf("[INFO] Client starting, press CTRL-C to stop")
	waitc := make(chan struct{})
	go func() {
		for {
			e, err := stream.Recv()
			if err == io.EOF {
				close(waitc)
				return
			}
			if err != nil {
				log.Fatalf("[FATAL] Event read failure: %s", err)
			}
			log.Printf("[INFO] Event received: %#v", e)
		}
	}()
	ic := make(chan os.Signal, 1)
	signal.Notify(ic, os.Interrupt)
	sig := <-ic
	log.Printf("[ERROR] Received %s, disconnecting", sig.String())
	stream.CloseSend()
	<-waitc
	log.Printf("[ERROR] Client shutdown success")
}
