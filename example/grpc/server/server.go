package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"

	"google.golang.org/grpc"

	"github.com/vancluever/fspubsub/example/grpc/events"
	"github.com/vancluever/fspubsub/sub"
)

type eventSubscriberServer struct{}

func (s *eventSubscriberServer) Subscribe(stream events.EventSubscriber_SubscribeServer) error {
	wd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("cannot get working directory: %s", err)
	}
	es, err := sub.NewSubscriber(wd, events.TestEvent{})
	if err != nil {
		return fmt.Errorf("cannot create subscriber: %s", err)
	}
	clientEOF := make(chan struct{})
	go func() {
		for {
			_, err := stream.Recv()
			if err == io.EOF {
				close(clientEOF)
				return
			}
		}
	}()
	go func() {
		for {
			select {
			case event := <-es.Queue:
				log.Printf("[INFO] Event %s received: %#v", event.ID, event.Data)
				d := event.Data.(events.TestEvent)
				stream.Send(&d)
			case <-es.Done:
				return
			case <-clientEOF:
				es.Close()
			}
		}
	}()
	log.Println("[INFO] Starting stream")
	if err := es.Subscribe(); err != nil {
		return err
	}
	log.Println("[INFO] Stream closed")
	return nil
}

func main() {
	s := eventSubscriberServer{}
	l, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("[FATAL] Error listening on TCP port 8080: %s", err)
	}
	grpcServer := grpc.NewServer()
	events.RegisterEventSubscriberServer(grpcServer, &s)
	ic := make(chan os.Signal, 1)
	stopping := make(chan struct{})
	stopped := make(chan struct{})
	signal.Notify(ic, os.Interrupt)
	go func() {
		sig := <-ic
		log.Printf("[ERROR] Received %s, beginning shutdown", sig.String())
		close(stopping)
		grpcServer.Stop()
		close(stopped)
	}()
	log.Printf("[INFO] Server starting, press CTRL-C to stop")
	err = grpcServer.Serve(l)

	// This little bit of error handling here needs to be present until some
	// issues get fixed either in grpc or in net. ErrServerStopped is not an
	// accurate way of checking if the gRPC server stopped due to a graceful
	// shutdown at this point in time.
	select {
	case <-stopping:
		<-stopped
	default:
		log.Fatalf("[FATAL] gRPC server error: %s", err)
	}
	log.Println("[ERROR] Normal shutdown - shutdown complete.")
}
