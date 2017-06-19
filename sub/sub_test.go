package sub

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/vancluever/fspubsub/pub"
)

type TestEvent struct {
	Text string
}

// sortableEvent is a sortable TestEvent.
type sortableEvent struct {
	TestEvent
}

// Less implements IndexedEvent for sortableEvent.
func (i sortableEvent) Less(j interface{}) bool {
	return i.Text < j.(sortableEvent).Text
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

type watchTestCase struct {
	Name      string
	EventType interface{}
	EventData interface{}
	Presub    func(string)
	Postsub   func(string)
	Pubfunc   func(string) error
	Err       string
}

var watchTestCases = []watchTestCase{
	{
		Name:      "basic success case",
		EventType: TestEvent{},
		EventData: TestEvent{Text: "foobar"},
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

// testWatchRun tests a watch run using the appropriate parameters and reutnrs
// the error. This is wrapped so that normal tests can be performed in addition
// to a race test.
func testWatchRun(tc watchTestCase) error {
	dir, _ := ioutil.TempDir("", "subtest")
	defer os.RemoveAll(dir)
	sub, err := NewSubscriber(dir, tc.EventType)
	if err != nil {
		return fmt.Errorf("bad: %s", err)
	}
	if tc.Presub != nil {
		tc.Presub(dir)
	}
	if tc.Postsub != nil {
		defer tc.Postsub(dir)
	}
	var timeout bool
	var actual Event
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	go func() {
		for {
			select {
			case actual = <-sub.Queue():
				sub.Close()
				return
			case <-ctx.Done():
				timeout = true
				sub.Close()
				return
			case <-sub.Done():
				sub.Close()
				return
			}
		}
	}()

	if err := sub.Subscribe(); err != nil {
		return fmt.Errorf("bad: %s", err)
	}

	var pubID string
	if tc.Pubfunc != nil {
		if err := tc.Pubfunc(dir); err != nil {
			return fmt.Errorf("bad: %s", err)
		}
	} else {
		p, err := pub.NewPublisher(dir, tc.EventType)
		if err != nil {
			return fmt.Errorf("bad: %s", err)
		}
		pubID, err = p.Publish(tc.EventData)
		if err != nil {
			return fmt.Errorf("bad: %s", err)
		}
	}

	<-sub.Done()

	switch {
	case timeout:
		return errors.New("timed out waiting for event")
	case sub.Error() != nil && tc.Err == "":
		return fmt.Errorf("bad: %s", err)
	case sub.Error() == nil && tc.Err != "":
		return errors.New("expected error, got none")
	case sub.Error() != nil && tc.Err != "":
		if !strings.Contains(sub.Error().Error(), tc.Err) {
			return fmt.Errorf("expected error to match %q, got %q", tc.Err, err)
		}
		return nil
	}

	expected := Event{
		ID:   pubID,
		Data: tc.EventData,
	}

	if !reflect.DeepEqual(expected, actual) {
		return fmt.Errorf("expected %#v, got %#v", tc.EventData, actual.Data)
	}
	return nil
}

func TestWatch(t *testing.T) {
	for _, tc := range watchTestCases {
		t.Run(tc.Name, func(t *testing.T) {
			if err := testWatchRun(tc); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// BenchmarkWatch runs all of the watch test cases in a stress-testing fashion,
// to try and detect races.
func BenchmarkWatch(b *testing.B) {
	for _, tc := range watchTestCases {
		b.Run(tc.Name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				if err := testWatchRun(tc); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestSubscribeCallback(t *testing.T) {
	tc := struct {
		EventType interface{}
		EventData interface{}
	}{
		EventType: TestEvent{},
		EventData: TestEvent{Text: "foobar"},
	}

	dir, _ := ioutil.TempDir("", "subtest")
	defer os.RemoveAll(dir)
	sub, err := NewSubscriber(dir, tc.EventType)
	if err != nil {
		t.Fatalf("bad: %s", err)
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
		p, pubErr = pub.NewPublisher(dir, tc.EventType)
		if pubErr != nil {
			return
		}
		pubID, pubErr = p.Publish(tc.EventData)
	}()
	cb := func(id string, data interface{}) {
		actual.ID = id
		actual.Data = data
		sub.Close()
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				timeout = true
				sub.Close()
				return
			}
		}
	}()
	err = sub.SubscribeCallback(cb)
	switch {
	case timeout:
		t.Fatal("timed out waiting for event")
	case pubErr != nil:
		t.Fatalf("error publishing event: %s", pubErr)
	case err != nil:
		t.Fatalf("bad: %s", err)
	}

	expected := Event{
		ID:   pubID,
		Data: tc.EventData,
	}

	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("expected %#v, got %#v", tc.EventData, actual.Data)
	}
}

func TestDump(t *testing.T) {
	cases := []struct {
		Name      string
		EventType interface{}
		EventData []interface{}
		Predump   func(string)
		Postdump  func(string)
		Pubfunc   func(string) error
		Err       string
	}{
		{
			Name:      "basic success case",
			EventType: sortableEvent{},
			EventData: []interface{}{sortableEvent{TestEvent: TestEvent{Text: "foobar"}}, sortableEvent{TestEvent{Text: "bazqux"}}},
		},
		{
			Name:      "readdir error",
			EventType: sortableEvent{},
			Predump:   func(d string) { os.Chmod(d, 0000) },
			Postdump:  func(d string) { os.Chmod(d, 0777) },
			Err:       "error reading event directory",
		},
		{
			Name:      "bad event permissions",
			EventType: sortableEvent{},
			Postdump:  func(d string) { os.Chmod(d+"/sortableEvent/bad", 0666) },
			Pubfunc: func(d string) error {
				return ioutil.WriteFile(d+"/sortableEvent/bad", []byte("{\"Text\": \"\"}"), 0000)
			},
			Err: "error reading event data at",
		},
		{
			Name:      "bad event data",
			EventType: sortableEvent{},
			Pubfunc:   func(d string) error { return ioutil.WriteFile(d+"/sortableEvent/bad", []byte("{\"Text\": 42}"), 0666) },
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
			var expected []Event
			if tc.Pubfunc != nil {
				if err := tc.Pubfunc(dir); err != nil {
					t.Fatalf("bad: %s", err)
				}
			} else {
				p, err := pub.NewPublisher(dir, tc.EventType)
				if err != nil {
					t.Fatalf("bad: %s", err)
				}
				for _, e := range tc.EventData {
					id, err := p.Publish(e)
					if err != nil {
						t.Fatalf("bad: %s", err)
					}
					expected = append(expected, Event{
						ID:   id,
						Data: e,
					})
				}
			}

			if tc.Predump != nil {
				tc.Predump(dir)
			}
			if tc.Postdump != nil {
				defer tc.Postdump(dir)
			}
			actual, err := sub.Dump()
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

			sort.Sort(eventSlice(expected))
			// actual needs to be sorted here as an unsorted Dump is non-deterministic.
			sort.Sort(eventSlice(actual))

			if !reflect.DeepEqual(expected, actual) {
				t.Fatalf("expected:\n\n%s\ngot:\n\n%s\n", spew.Sdump(expected), spew.Sdump(actual))
			}
		})
	}
}
