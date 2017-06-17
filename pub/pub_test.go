package pub

import (
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"
)

type TestEvent struct {
	Text string
}

type BadEvent struct {
	Text string
	Ch   chan interface{}
}

// badRander is a io.Reader that just spits out errors, to simulate a RNG error on UUID generation.
type badRander struct{}

// Read just returns an error, to mock an io.Reader.
func (r *badRander) Read(p []byte) (int, error) {
	return 0, errors.New("mock reader, always errors")
}

// nonRander is a io.Reader that just spits out zeros, to ensure identical
// UUIDs are generated for UUID generation.
type nonRander struct{}

// Read returns a single zero.
func (r *nonRander) Read(p []byte) (int, error) {
	for n := range p {
		p[n] = 0
	}
	return len(p), nil
}

func TestNewPublisher(t *testing.T) {
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
			PathFunc:    func() string { p, _ := ioutil.TempDir("", "pubtest"); os.Mkdir(p+"/TestEvent", 0777); return p },
			PathCleanup: func(p string) { os.RemoveAll(p) },
		},
		{
			Name:        "create if dir does not exist",
			EventType:   TestEvent{},
			PathFunc:    func() string { p, _ := ioutil.TempDir("", "pubtest"); return p },
			PathCleanup: func(p string) { os.RemoveAll(p) },
		},
		{
			Name:        "nil event",
			EventType:   nil,
			PathFunc:    func() string { p, _ := ioutil.TempDir("", "pubtest"); return p },
			PathCleanup: func(p string) { os.RemoveAll(p) },
			Err:         "event cannot be nil",
		},
		{
			Name:      "not a directory",
			EventType: TestEvent{},
			PathFunc: func() string {
				p, _ := ioutil.TempDir("", "pubtest")
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
				p, _ := ioutil.TempDir("", "pubtest")
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
			PathFunc:    func() string { p, _ := ioutil.TempDir("", "pubtest"); os.Chmod(p, 0000); return p },
			PathCleanup: func(p string) { os.Chmod(p, 0777); os.RemoveAll(p) },
			Err:         "could not stat dir",
		},
	}

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			dir := tc.PathFunc()
			defer tc.PathCleanup(dir)
			pub, err := NewPublisher(dir, tc.EventType)
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
			expected := &Publisher{
				dir:       dir + "/" + reflect.TypeOf(tc.EventType).Name(),
				eventType: reflect.TypeOf(tc.EventType),
			}

			if !reflect.DeepEqual(expected, pub) {
				t.Fatalf("expected %#v, got %#v", expected, pub)
			}
		})
	}
}

func TestPublish(t *testing.T) {
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
			Err:       "event of type pub.BadEvent does not match publisher type pub.TestEvent",
		},
		{
			Name:      "json marshaling error",
			EventType: BadEvent{},
			EventData: BadEvent{Text: "foobar", Ch: make(chan interface{})},
			Err:       "could not marshal event data",
		},
		{
			Name:      "id generation error",
			EventType: TestEvent{},
			EventData: TestEvent{Text: "foobar"},
			Rander:    &badRander{},
			Err:       "could not generate ID: mock reader, always errors",
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
			dir, _ := ioutil.TempDir("", "pubtest")
			defer os.RemoveAll(dir)
			pub, err := NewPublisher(dir, tc.EventType)
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
			id, err := pub.Publish(tc.EventData)
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
			actual, _ := ioutil.ReadFile(dir + "/" + reflect.TypeOf(tc.EventType).Name() + "/" + id)

			if !reflect.DeepEqual(expected, actual) {
				t.Fatalf("expected %q, got %q", expected, actual)
			}
		})
	}
}
