package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/gopherjs/vecty"
	"github.com/gopherjs/vecty/elem"
	"github.com/gopherjs/vecty/event"
	"github.com/gopherjs/websocket"
	"github.com/vancluever/fspubsub/example/grpc/events"
)

// displayLines indicates the lines to display. The last amount of lines equal
// to this amount are displayed.
const displayLines = 20

// buttonToggleLableStartStop is a simple map[bool]string that prints a button
// label appropriate to a button's toggle state.
var buttonToggleLableStartStop = map[bool]string{
	false: "Start",
	true:  "Stop",
}

type preformattedBuffer struct {
	vecty.Core
	buffer string
}

func (c *preformattedBuffer) Render() *vecty.HTML {
	// Empty data if nothing in buffer
	if c.buffer == "" {
		return elem.Preformatted()
	}

	// Get the last lines in the buffer to the limit defined by displayLines.
	bs := strings.Split(c.buffer, "\n")
	offset := len(bs) - displayLines
	if offset < 0 {
		offset = 0
	}
	dt := strings.Join(bs[offset:], "\n")
	return elem.Preformatted(vecty.Text(dt))
}

func (c *preformattedBuffer) Println(a ...interface{}) {
	c.buffer += fmt.Sprintln(a...)
	vecty.Rerender(c)
}

func (c *preformattedBuffer) Printf(format string, a ...interface{}) {
	c.buffer += fmt.Sprintln(fmt.Sprintf(format, a...))
	vecty.Rerender(c)
}

type toggleButton struct {
	vecty.Core
	state bool
	cb    func(*vecty.Event, *toggleButton)
}

func (c *toggleButton) StateToggle() {
	c.state = !c.state
	vecty.Rerender(c)
}

func (c *toggleButton) StateOff() {
	c.state = false
	vecty.Rerender(c)
}

func (c *toggleButton) StateOn() {
	c.state = true
	vecty.Rerender(c)
}

func (c *toggleButton) Render() *vecty.HTML {
	return elem.Button(
		vecty.Text(buttonToggleLableStartStop[c.state]),
		event.Click(func(e *vecty.Event) {
			c.cb(e, c)
		}),
	)
}

type dom struct {
	vecty.Core
}

func (c *dom) Render() *vecty.HTML {
	pb := &preformattedBuffer{}
	return elem.Body(
		elem.Heading1(vecty.Text("Event Stream Web Demo")),
		elem.Label(vecty.Text("Press the button to toggle event start/stop: ")),
		&toggleButton{
			cb: func(e *vecty.Event, c *toggleButton) {
				switch c.state {
				case false:
					go func() {
						conn, err := websocket.Dial("ws://localhost:8080/ws")
						if err != nil {
							pb.Printf("[FATAL] Could not connect to websocket ws://localhost:8080/ws: %s", err)
							c.StateOff()
							return
						}
						waitc := make(chan struct{})
						stream := json.NewDecoder(conn)
						pb.Println("[INFO] Event stream started.")
						c.StateOn()
						go func() {
							for {
								var e events.TestEvent
								if err := stream.Decode(&e); err != nil {
									pb.Printf("[ERROR] Event read error: %s", err)
									close(waitc)
									return
								}
								pb.Printf("[INFO] Event received: %#v", e)
							}
						}()
						go func() {
							for {
								if !c.state {
									conn.Close()
									return
								}
								time.Sleep(time.Millisecond * 100)
							}
						}()
						<-waitc
						c.StateOff()
						pb.Printf("[ERROR] Client shutdown")
					}()
				case true:
					pb.Println("[ERROR] Stop request received, disconnecting")
					c.StateOff()
				}
			}},
		pb,
	)
}

func main() {
	vecty.SetTitle("Event Stream Web Demo")
	vecty.RenderBody(&dom{})
}
