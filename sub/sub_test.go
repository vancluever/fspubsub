package sub

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/vancluever/fspubsub/pub"
	"github.com/vancluever/fspubsub/store"
)

type TestEvent struct {
	Text string
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

			expectedStream, err := store.NewStream(dir, tc.EventType)
			if err != nil {
				t.Fatalf("bad: %s", err)
			}

			if !reflect.DeepEqual(expectedStream, sub.Stream) {
				t.Fatalf("expected:\n\n%s\ngot:\n\n%s\n", expectedStream, sub.Stream)
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
		return fmt.Errorf("NewSubscriber: bad: %s", err)
	}
	if tc.Presub != nil {
		tc.Presub(dir)
	}
	if tc.Postsub != nil {
		defer tc.Postsub(dir)
	}
	var timeout bool
	var actual store.Event
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

	var pubID string
	if tc.Pubfunc != nil {
		if err := tc.Pubfunc(dir); err != nil {
			return fmt.Errorf("Pubfunc: bad: %s", err)
		}
	} else {
		p, err := pub.NewPublisher(dir, tc.EventType)
		if err != nil {
			return fmt.Errorf("NewPublisher: bad: %s", err)
		}
		pubID, err = p.Publish(tc.EventData)
		if err != nil {
			return fmt.Errorf("Publish: bad: %s", err)
		}
	}

	<-sub.Done()

	switch {
	case timeout:
		return errors.New("timed out waiting for event")
	case sub.Error() != nil && tc.Err == "":
		return fmt.Errorf("Subscriber error: bad: %s", err)
	case sub.Error() == nil && tc.Err != "":
		return errors.New("expected error, got none")
	case sub.Error() != nil && tc.Err != "":
		if !strings.Contains(sub.Error().Error(), tc.Err) {
			return fmt.Errorf("expected error to match %q, got %q", tc.Err, err)
		}
		return nil
	}

	expected := store.Event{
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
