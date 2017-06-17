package sub

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/vancluever/fspubsub/pub"
)

type TestEvent struct {
	Text string
}

// seedTestEventStore pre-populates a test event stream with some events.
func seedTestEventStore(es []TestEvent) (string, error) {
	dir, err := ioutil.TempDir("", "subtest")
	if err != nil {
		return "", err
	}
	p, err := pub.NewPublisher(dir, TestEvent{})
	if err != nil {
		return "", err
	}
	for _, e := range es {
		if _, err := p.Publish(e); err != nil {
			return "", err
		}
	}
	return dir, nil
}

func TestNewSubscriber(t *testing.T) {
	cases := []struct {
		Name        string
		EventType   interface{}
		PathFunc    func() string
		PathCleanup func(string)
		Err         string
	}{
		{
			Name:        "basic success case",
			EventType:   TestEvent{},
			PathFunc:    func() string { p, _ := ioutil.TempDir("", "subtest"); os.Mkdir(p+"/TestEvent", 0777); return p },
			PathCleanup: func(p string) { os.RemoveAll(p) },
		},
		{
			Name:        "create if dir does not exist",
			EventType:   TestEvent{},
			PathFunc:    func() string { p, _ := ioutil.TempDir("", "subtest"); return p },
			PathCleanup: func(p string) { os.RemoveAll(p) },
		},
		{
			Name:        "nil event",
			EventType:   nil,
			PathFunc:    func() string { p, _ := ioutil.TempDir("", "subtest"); return p },
			PathCleanup: func(p string) { os.RemoveAll(p) },
			Err:         "event cannot be nil",
		},
		{
			Name:      "not a directory",
			EventType: TestEvent{},
			PathFunc: func() string {
				p, _ := ioutil.TempDir("", "subtest")
				ioutil.WriteFile(p+"/TestEvent", []byte{}, 0666)
				return p
			},
			PathCleanup: func(p string) { os.RemoveAll(p) },
			Err:         "TestEvent exists and is not a directory",
		},
		{
			Name:      "stat error, parent dir permission denied",
			EventType: TestEvent{},
			PathFunc: func() string {
				p, _ := ioutil.TempDir("", "subtest")
				d := p + "/foobar"
				os.Mkdir(d, 0777)
				os.Chmod(d, 0000)
				return d
			},
			PathCleanup: func(p string) { os.Chmod(p, 0777); os.RemoveAll(filepath.Clean(p + "/..")) },
			Err:         "could not stat dir",
		},
		{
			Name:        "stat error, general",
			EventType:   TestEvent{},
			PathFunc:    func() string { p, _ := ioutil.TempDir("", "subtest"); os.Chmod(p, 0000); return p },
			PathCleanup: func(p string) { os.Chmod(p, 0777); os.RemoveAll(p) },
			Err:         "could not stat dir",
		},
	}

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			dir := tc.PathFunc()
			defer tc.PathCleanup(dir)
			sub, err := NewSubscriber(dir, tc.EventType)
			switch {
			case err != nil && tc.Err == "":
				t.Fatalf("bad: %s", err)
			case err == nil && tc.Err != "":
				t.Fatal("expected error, got none")
			case err != nil && tc.Err != "":
				if !strings.Contains(err.Error(), tc.Err) {
					t.Fatalf("expected error to match %q, got %q", tc.Err, err)
				}
				return
			}

			expectedDir := filepath.Clean(dir) + "/" + reflect.TypeOf(tc.EventType).Name()
			expectedEventType := reflect.TypeOf(tc.EventType)

			if expectedDir != sub.dir {
				t.Fatalf("expected dir to be %q, got %q", expectedDir, sub.dir)
			}
			if expectedEventType != sub.eventType {
				t.Fatalf("expected eventType to be %s, got %s", expectedEventType, sub.eventType)
			}
		})
	}
}

func TestSubscribe(t *testing.T) {
	cases := []struct {
		Name      string
		EventType interface{}
		EventData interface{}
		Presub    func(string)
		Postsub   func(string)
		Pubfunc   func(string) error
		Err       string
	}{
		{
			Name:      "basic success case",
			EventType: TestEvent{},
			EventData: TestEvent{Text: "foobar"},
		},
		{
			Name:      "directory watch error",
			EventType: TestEvent{},
			EventData: TestEvent{Text: "foobar"},
			Presub:    func(d string) { os.Chmod(d, 0000) },
			Postsub:   func(d string) { os.Chmod(d, 0777) },
			Err:       "error watching directory",
		},
		{
			Name:      "bad event permissions",
			EventType: TestEvent{},
			Postsub:   func(d string) { os.Chmod(d+"/TestEvent/bad", 0666) },
			Pubfunc:   func(d string) error { return ioutil.WriteFile(d+"/TestEvent/bad", []byte("{\"Text\": \"\"}"), 0000) },
			Err:       "error reading event data at",
		},
		{
			Name:      "bad event data",
			EventType: TestEvent{},
			Pubfunc:   func(d string) error { return ioutil.WriteFile(d+"/TestEvent/bad", []byte("{\"Text\": 42}"), 0666) },
			Err:       "error unmarshaling event data from",
		},
	}

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			dir, _ := ioutil.TempDir("", "subtest")
			defer os.RemoveAll(dir)
			sub, err := NewSubscriber(dir, tc.EventType)
			if err != nil {
				t.Fatalf("bad: %s", err)
			}
			if tc.Presub != nil {
				tc.Presub(dir)
			}
			if tc.Postsub != nil {
				defer tc.Postsub(dir)
			}
			var timeout bool
			var pubErr error
			var pubID string
			var actual Event
			// Set the timeout on this test low - 10 seconds should be plenty.
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			go func() {
				var p *pub.Publisher
				if tc.Pubfunc != nil {
					pubErr = tc.Pubfunc(dir)
					return
				}
				p, pubErr = pub.NewPublisher(dir, tc.EventType)
				if err != nil {
					return
				}
				pubID, pubErr = p.Publish(tc.EventData)
			}()
			go func() {
				for {
					select {
					case actual = <-sub.Queue:
						sub.Close()
						return
					case <-ctx.Done():
						timeout = true
						sub.Close()
						return
					}
				}
			}()

			err = sub.Subscribe()
			switch {
			case timeout:
				t.Fatal("timed out waiting for event")
			case pubErr != nil:
				t.Fatalf("error publishing event: %s", pubErr)
			case err != nil && tc.Err == "":
				t.Fatalf("bad: %s", err)
			case err == nil && tc.Err != "":
				t.Fatal("expected error, got none")
			case err != nil && tc.Err != "":
				if !strings.Contains(err.Error(), tc.Err) {
					t.Fatalf("expected error to match %q, got %q", tc.Err, err)
				}
				return
			}

			expected := Event{
				ID:   pubID,
				Data: tc.EventData,
			}

			if !reflect.DeepEqual(expected, actual) {
				t.Fatalf("expected %#v, got %#v", tc.EventData, actual.Data)
			}
		})
	}
}
