package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"

	"golang.org/x/sys/unix"

	"github.com/gorilla/websocket"
	"github.com/vancluever/fspubsub/pub"
	"github.com/vancluever/fspubsub/sub"
)

type TestEvent struct {
	Text string
}

var wd string

func init() {
	var err error
	wd, err = os.Getwd()
	if err != nil {
		log.Fatalf("[FATAL] Cannot get working directory: %s", err)
	}
}

func handleSub(w http.ResponseWriter, r *http.Request) {
	up := websocket.Upgrader{}
	conn, err := up.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[ERROR] Websocket upgrade error: %s", err)
		http.Error(w, fmt.Sprintf("websocket upgrade error: %s", err), 500)
	}
	es, err := sub.NewSubscriber(wd, TestEvent{})
	if err != nil {
		log.Printf("[ERROR] Cannot create subscriber: %s", err)
		http.Error(w, fmt.Sprintf("cannot create subscriber: %s", err), 500)
	}
	clientEOF := make(chan struct{})
	go func() {
		for {
			if _, _, err := conn.NextReader(); err != nil {
				log.Printf("[ERROR] Client EOF")
				es.Close()
				close(clientEOF)
				return
			}
		}
	}()
	go func() {
		for {
			select {
			case <-clientEOF:
				return
			case event := <-es.Queue:
				log.Printf("[INFO] Event %s received: %#v", event.ID, event.Data)
				if err := conn.WriteJSON(event.Data); err != nil {
					log.Printf("[ERROR] Stream send error: %s", err)
					return
				}
			case <-es.Done:
				return
			}
		}
	}()
	log.Println("[INFO] Starting stream")
	if err := es.Subscribe(); err != nil {
		log.Printf("[ERROR] Cannot subscribe: %s", err)
		http.Error(w, fmt.Sprintf("cannot subscribe: %s", err), 500)
	}
	if err := conn.Close(); err != nil {
		log.Printf("[ERROR] Error closing websocket: %s", err)
	}
	log.Println("[INFO] Stream closed")
}

func handlePub(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" || r.Header.Get("Content-Type") != "application/json" {
		log.Println("[ERROR] Bad request: Method not POST or Content-Type not application/json on /publish")
		http.Error(w, "bad request: method not POST or Content-Type not application/json on /publish", 400)
		return
	}
	dec := json.NewDecoder(r.Body)
	var e TestEvent
	if err := dec.Decode(&e); err != nil {
		log.Printf("[ERROR] Bad request: JSON processing error: %s", err)
		http.Error(w, fmt.Sprintf("bad request: JSON processing error: %s", err), 400)
		return
	}
	p, err := pub.NewPublisher(wd, TestEvent{})
	if err != nil {
		log.Printf("[ERROR] Could not create publisher: %s", err)
		http.Error(w, fmt.Sprintf("could not create publisher: %s", err), 500)
		return
	}
	resp := make(map[string]string)
	resp["id"], err = p.Publish(e)
	if err != nil {
		log.Printf("[ERROR] Error publishing event: %s", err)
		http.Error(w, fmt.Sprintf("error publishing event: %s", err), 500)
		return
	}
	w.Header().Add("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.Encode(resp)
}

func main() {
	log.Println("[INFO] Server starting...")

	ln, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("[FATAL] Error listening on %s: %s", ln, err)
	}
	log.Printf("[INFO] Listening on %s", ln.(*net.TCPListener).Addr().String())
	http.HandleFunc("/publish", handlePub)
	http.HandleFunc("/ws", handleSub)
	http.HandleFunc("/", http.FileServer(http.Dir(wd+"/client")).ServeHTTP)
	log.Printf("[INFO] Press CTRL-C or send SIGTERM to close the server")
	stopping := make(chan struct{})
	stopped := make(chan struct{})

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, unix.SIGTERM)
	go func() {
		s := <-c
		log.Printf("[ERROR] Received %s, shutting down", s.String())
		close(stopping)
		ln.Close()
		close(stopped)
	}()
	err = http.Serve(ln, nil)
	select {
	case <-stopping:
		<-stopped
	default:
		log.Fatalf("[FATAL] %s", err)
	}
	log.Println("[ERROR] Normal server shutdown complete")
}
