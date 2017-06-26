package store

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

type TestEvent struct {
	Text string
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
