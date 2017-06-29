package store

import (
	"encoding/json"
	"io"
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

type BadEvent struct {
	Text string
	Ch   chan interface{}
}

// nonRander is an io.Reader that just spits out zeros, to ensure identical
// UUIDs are generated for UUID generation.
type nonRander struct{}

// Read returns a single zero.
func (r *nonRander) Read(p []byte) (int, error) {
	for n := range p {
		p[n] = 0
	}
	return len(p), nil
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

func TestWriteEvent(t *testing.T) {
	cases := []struct {
		Name        string
		EventType   interface{}
		EventData   interface{}
		Rander      io.Reader
		Prepub      func(string)
		Postpub     func(string)
		Err         string
		IsCollision bool
	}{
		{
			Name:      "basic success case",
			EventType: TestEvent{},
			EventData: TestEvent{Text: "foobar"},
		},
		{
			Name:      "mismatched event type",
			EventType: TestEvent{},
			EventData: BadEvent{Text: "foobar", Ch: make(chan interface{})},
			Err:       "event of type store.BadEvent does not match stream type store.TestEvent",
		},
		{
			Name:      "json marshaling error",
			EventType: BadEvent{},
			EventData: BadEvent{Text: "foobar", Ch: make(chan interface{})},
			Err:       "could not marshal event data",
		},
		{
			Name:      "id collision",
			EventType: TestEvent{},
			EventData: TestEvent{Text: "foobar"},
			Rander:    &nonRander{},
			Prepub: func(d string) {
				id := uuid.New()
				ioutil.WriteFile(d+"/TestEvent/"+id.String(), []byte{}, 0666)
			},
			Err:         "id collision",
			IsCollision: true,
		},
		{
			Name:      "file write error",
			EventType: TestEvent{},
			EventData: TestEvent{Text: "foobar"},
			Prepub:    func(d string) { os.Chmod(d, 0000) },
			Postpub:   func(d string) { os.Chmod(d, 0777) },
			Err:       "error writing event to file",
		},
	}

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			dir, _ := ioutil.TempDir("", "storetest")
			defer os.RemoveAll(dir)
			s, err := NewStream(dir, tc.EventType)
			if err != nil {
				t.Fatalf("bad: %s", err)
			}
			if tc.Rander != nil {
				uuid.SetRand(tc.Rander)
			}
			if tc.Prepub != nil {
				tc.Prepub(dir)
			}
			if tc.Postpub != nil {
				defer tc.Postpub(dir)
			}
			id, err := uuid.NewRandom()
			if err != nil {
				t.Fatalf("bad: could not generate ID: %s", err)
			}
			if tc.Rander != nil {
				// Reset the rander here if we set it before so that it doesn't break
				// other tests.
				uuid.SetRand(nil)
			}
			err = s.WriteEvent(id.String(), tc.EventData)
			switch {
			case err != nil && tc.Err == "":
				t.Fatalf("bad: %s", err)
			case err == nil && tc.Err != "":
				t.Fatal("expected error, got none")
			case err != nil && tc.Err != "":
				if !strings.Contains(err.Error(), tc.Err) {
					t.Fatalf("expected error to match %q, got %q", tc.Err, err)
				}
				if tc.IsCollision {
					if _, ok := err.(IDCollisionError); !ok {
						t.Fatalf("Error should have been type IDCollisionError, is %s instead", reflect.TypeOf(err).String())
					}
				}
				return
			}
			expected, _ := json.Marshal(tc.EventData)
			actual, _ := ioutil.ReadFile(dir + "/" + reflect.TypeOf(tc.EventType).Name() + "/" + id.String())

			if !reflect.DeepEqual(expected, actual) {
				t.Fatalf("expected %q, got %q", expected, actual)
			}
		})
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
			dir, _ := ioutil.TempDir("", "storetest")
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

func TestDumpSorted(t *testing.T) {
	cases := []struct {
		Name      string
		EventType interface{}
		EventData []interface{}
		Predump   func(string)
		Postdump  func(string)
		Err       string
		Panic     bool
		Reverse   bool
	}{
		{
			Name:      "basic success case",
			EventType: sortableEvent{},
			EventData: []interface{}{
				sortableEvent{TestEvent: TestEvent{Text: "1"}},
				sortableEvent{TestEvent: TestEvent{Text: "0"}},
				sortableEvent{TestEvent: TestEvent{Text: "3"}},
				sortableEvent{TestEvent: TestEvent{Text: "2"}},
				sortableEvent{TestEvent: TestEvent{Text: "5"}},
				sortableEvent{TestEvent: TestEvent{Text: "4"}},
				sortableEvent{TestEvent: TestEvent{Text: "7"}},
				sortableEvent{TestEvent: TestEvent{Text: "6"}},
				sortableEvent{TestEvent: TestEvent{Text: "9"}},
				sortableEvent{TestEvent: TestEvent{Text: "8"}},
			},
		},
		{
			Name:      "basic error case",
			Predump:   func(d string) { os.Chmod(d, 0000) },
			Postdump:  func(d string) { os.Chmod(d, 0777) },
			EventType: sortableEvent{},
			EventData: []interface{}{
				sortableEvent{TestEvent: TestEvent{Text: "1"}},
				sortableEvent{TestEvent: TestEvent{Text: "0"}},
				sortableEvent{TestEvent: TestEvent{Text: "3"}},
				sortableEvent{TestEvent: TestEvent{Text: "2"}},
				sortableEvent{TestEvent: TestEvent{Text: "5"}},
				sortableEvent{TestEvent: TestEvent{Text: "4"}},
				sortableEvent{TestEvent: TestEvent{Text: "7"}},
				sortableEvent{TestEvent: TestEvent{Text: "6"}},
				sortableEvent{TestEvent: TestEvent{Text: "9"}},
				sortableEvent{TestEvent: TestEvent{Text: "8"}},
			},
			Err: "could not stat dir",
		},
		{
			Name:      "should panic",
			EventType: TestEvent{},
			EventData: []interface{}{
				TestEvent{Text: "1"},
				TestEvent{Text: "0"},
				TestEvent{Text: "3"},
				TestEvent{Text: "2"},
				TestEvent{Text: "5"},
				TestEvent{Text: "4"},
				TestEvent{Text: "7"},
				TestEvent{Text: "6"},
				TestEvent{Text: "9"},
				TestEvent{Text: "8"},
			},
			Err:   "does not implement IndexedEvent",
			Panic: true,
		},
		{
			Name:      "reverse - basic success case",
			EventType: sortableEvent{},
			EventData: []interface{}{
				sortableEvent{TestEvent: TestEvent{Text: "1"}},
				sortableEvent{TestEvent: TestEvent{Text: "0"}},
				sortableEvent{TestEvent: TestEvent{Text: "3"}},
				sortableEvent{TestEvent: TestEvent{Text: "2"}},
				sortableEvent{TestEvent: TestEvent{Text: "5"}},
				sortableEvent{TestEvent: TestEvent{Text: "4"}},
				sortableEvent{TestEvent: TestEvent{Text: "7"}},
				sortableEvent{TestEvent: TestEvent{Text: "6"}},
				sortableEvent{TestEvent: TestEvent{Text: "9"}},
				sortableEvent{TestEvent: TestEvent{Text: "8"}},
			},
			Reverse: true,
		},
		{
			Name:      "reverse - basic error case",
			Predump:   func(d string) { os.Chmod(d, 0000) },
			Postdump:  func(d string) { os.Chmod(d, 0777) },
			EventType: sortableEvent{},
			EventData: []interface{}{
				sortableEvent{TestEvent: TestEvent{Text: "1"}},
				sortableEvent{TestEvent: TestEvent{Text: "0"}},
				sortableEvent{TestEvent: TestEvent{Text: "3"}},
				sortableEvent{TestEvent: TestEvent{Text: "2"}},
				sortableEvent{TestEvent: TestEvent{Text: "5"}},
				sortableEvent{TestEvent: TestEvent{Text: "4"}},
				sortableEvent{TestEvent: TestEvent{Text: "7"}},
				sortableEvent{TestEvent: TestEvent{Text: "6"}},
				sortableEvent{TestEvent: TestEvent{Text: "9"}},
				sortableEvent{TestEvent: TestEvent{Text: "8"}},
			},
			Err:     "could not stat dir",
			Reverse: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			dir, _ := ioutil.TempDir("", "storetest")
			defer os.RemoveAll(dir)
			var expected []Event
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

			if tc.Predump != nil {
				tc.Predump(dir)
			}
			if tc.Postdump != nil {
				defer tc.Postdump(dir)
			}

			defer func() {
				if r := recover(); r != nil {
					switch tc.Panic {
					case false:
						t.Fatalf("panic: %s", r)
					case true:
						if !strings.Contains(r.(error).Error(), tc.Err) {
							t.Fatalf("expected panic error to match %q, got %q", tc.Err, r)
						}
						return
					}
				}
			}()
			var f func(string, interface{}) ([]Event, error)
			if tc.Reverse {
				f = DumpSortedReverse
			} else {
				f = DumpSorted
			}
			actual, err := f(dir, tc.EventType)
			switch {
			case tc.Panic:
				t.Fatal("expected panic, got none")
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

			if tc.Reverse {
				sort.Sort(sort.Reverse(eventSlice(expected)))
			} else {
				sort.Sort(eventSlice(expected))
			}

			if !reflect.DeepEqual(expected, actual) {
				t.Fatalf("expected:\n\n%s\ngot:\n\n%s\n", spew.Sdump(expected), spew.Sdump(actual))
			}
		})
	}
}

func TestFetch(t *testing.T) {
	cases := []struct {
		Name      string
		EventType interface{}
		EventData interface{}
		Prefetch  func(string)
		Postfetch func(string)
		Err       string
	}{
		{
			Name:      "basic success case",
			EventType: TestEvent{},
			EventData: TestEvent{Text: "foobar"},
		},
		{
			Name:      "basic error case",
			Prefetch:  func(d string) { os.Chmod(d, 0000) },
			Postfetch: func(d string) { os.Chmod(d, 0777) },
			EventType: TestEvent{},
			EventData: TestEvent{Text: "foobar"},
			Err:       "could not stat dir",
		},
	}

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			dir, _ := ioutil.TempDir("", "storetest")
			defer os.RemoveAll(dir)
			s, err := NewStream(dir, tc.EventType)
			if err != nil {
				t.Fatalf("bad: %s", err)
			}
			id, err := uuid.NewRandom()
			if err != nil {
				t.Fatalf("bad: could not generate ID: %s", err)
			}
			if err := s.WriteEvent(id.String(), tc.EventData); err != nil {
				t.Fatalf("bad: %s", err)
			}
			expected := Event{
				ID:   id.String(),
				Data: tc.EventData,
			}

			if tc.Prefetch != nil {
				tc.Prefetch(dir)
			}
			if tc.Postfetch != nil {
				defer tc.Postfetch(dir)
			}

			actual, err := Fetch(dir, tc.EventType, id.String())
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

			if !reflect.DeepEqual(expected, actual) {
				t.Fatalf("expected:\n\n%s\ngot:\n\n%s\n", spew.Sdump(expected), spew.Sdump(actual))
			}
		})
	}
}
