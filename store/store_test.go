package store

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/google/uuid"
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

func TestNewStream(t *testing.T) {
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
			PathFunc:    func() string { p, _ := ioutil.TempDir("", "storetest"); os.Mkdir(p+"/TestEvent", 0777); return p },
			PathCleanup: func(p string) { os.RemoveAll(p) },
		},
		{
			Name:        "create if dir does not exist",
			EventType:   TestEvent{},
			PathFunc:    func() string { p, _ := ioutil.TempDir("", "storetest"); return p },
			PathCleanup: func(p string) { os.RemoveAll(p) },
		},
		{
			Name:        "nil event",
			EventType:   nil,
			PathFunc:    func() string { p, _ := ioutil.TempDir("", "storetest"); return p },
			PathCleanup: func(p string) { os.RemoveAll(p) },
			Err:         "event cannot be nil",
		},
		{
			Name:      "not a directory",
			EventType: TestEvent{},
			PathFunc: func() string {
				p, _ := ioutil.TempDir("", "storetest")
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
				p, _ := ioutil.TempDir("", "storetest")
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
			PathFunc:    func() string { p, _ := ioutil.TempDir("", "storetest"); os.Chmod(p, 0000); return p },
			PathCleanup: func(p string) { os.Chmod(p, 0777); os.RemoveAll(p) },
			Err:         "could not stat dir",
		},
	}

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			dir := tc.PathFunc()
			defer tc.PathCleanup(dir)
			actual, err := NewStream(dir, tc.EventType)
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
			expected := &Stream{
				dir:       filepath.Clean(dir) + "/" + reflect.TypeOf(tc.EventType).Name(),
				eventType: reflect.TypeOf(tc.EventType),
			}

			if !reflect.DeepEqual(expected, actual) {
				t.Fatalf("expected %#v, got %#v", expected, actual)
			}
		})
	}
}

func TestDir(t *testing.T) {
	expected := "foo/bar"
	stream := &Stream{
		dir:       expected,
		eventType: reflect.TypeOf(TestEvent{}),
	}

	if expected != stream.Dir() {
		t.Fatalf("Expected %s, got %s", expected, stream.Dir())
	}
}

func TestEventType(t *testing.T) {
	expected := reflect.TypeOf(TestEvent{})
	stream := &Stream{
		dir:       "foo/bar",
		eventType: expected,
	}
	if expected != stream.EventType() {
		t.Fatalf("Expected %s, got %s", expected, stream.EventType())
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
			Err:       "could not stat dir",
		},
		{
			Name:      "bad event permissions",
			EventType: sortableEvent{},
			Postdump:  func(d string) { os.Chmod(d+"/sortableEvent/bad", 0666) },
			Pubfunc: func(d string) error {
				err := os.MkdirAll(d+"/sortableEvent", 0777)
				if err != nil {
					return err
				}
				return ioutil.WriteFile(d+"/sortableEvent/bad", []byte("{\"Text\": \"\"}"), 0000)
			},
			Err: "error reading event data at",
		},
		{
			Name:      "bad event data",
			EventType: sortableEvent{},
			Pubfunc: func(d string) error {
				err := os.MkdirAll(d+"/sortableEvent", 0777)
				if err != nil {
					return err
				}
				return ioutil.WriteFile(d+"/sortableEvent/bad", []byte("{\"Text\": 42}"), 0666)
			},
			Err: "error unmarshaling event data from",
		},
	}

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			dir, _ := ioutil.TempDir("", "subtest")
			defer os.RemoveAll(dir)
			var expected []Event
			if tc.Pubfunc != nil {
				if err := tc.Pubfunc(dir); err != nil {
					t.Fatalf("bad: %s", err)
				}
			} else {
				s, err := NewStream(dir, tc.EventType)
				if err != nil {
					t.Fatalf("bad: %s", err)
				}
				for _, e := range tc.EventData {
					id, err := uuid.NewRandom()
					if err != nil {
						t.Fatalf("bad: could not generate ID: %s", err)
					}
					if err := s.WriteEvent(id.String(), e); err != nil {
						t.Fatalf("bad: %s", err)
					}
					expected = append(expected, Event{
						ID:   id.String(),
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
			actual, err := Dump(dir, tc.EventType)
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
