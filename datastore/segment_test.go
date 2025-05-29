package datastore

import (
	"fmt"
	"os"
	"testing"
)

const testMaxSegmentSize = 50

func TestSegmentsCreation(t *testing.T) {
	dir := "testdata"
	os.RemoveAll(dir)
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("failed to create a catalogue testdata: %v", err)
	}

	ds, err := NewSegmentedDatastore(dir, testMaxSegmentSize)
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()

	totalEntries := 50
	for i := 0; i < totalEntries; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		if err := ds.Put(key, value); err != nil {
			t.Fatalf("error during Put(%s): %v", key, err)
		}
	}

	if len(ds.segments) < 2 {
		t.Errorf("at least 2 segments were waited, but got %d", len(ds.segments))
	}

	for i := 0; i < totalEntries; i++ {
		key := fmt.Sprintf("key%d", i)
		expected := fmt.Sprintf("value%d", i)
		value, err := ds.Get(key)
		if err != nil {
			t.Errorf("Get(%s): %v", key, err)
		}
		if value != expected {
			t.Errorf("%s: were waited %s, got %s", key, expected, value)
		}
	}
}

func TestMergeSegments(t *testing.T) {
	dir := t.TempDir()

	ds, err := NewSegmentedDatastore(dir, testMaxSegmentSize)
	if err != nil {
		t.Fatalf("failed to createdatastore: %v", err)
	}
	defer ds.Close()

	key := "key"

	if err := ds.Put(key, "old"); err != nil {
		t.Fatalf("failed to write old value: %v", err)
	}
	ds.segments[len(ds.segments)-1].Close()

	if err := ds.createNewSegment(); err != nil {
		t.Fatalf("failed to create new segment: %v", err)
	}

	if err := ds.Put(key, "new"); err != nil {
		t.Fatalf("failed to write a new value: %v", err)
	}
	ds.segments[len(ds.segments)-1].Close()

	for i, seg := range ds.segments {
		_, err := seg.ReadAll()
		if err != nil {
			t.Errorf("segment %d (%s): error of reading: %v", i, seg.filename, err)
		}
	}

	if err := ds.Merge(); err != nil {
		t.Fatalf("merge didn't happen: %v", err)
	}

	got, err := ds.Get(key)
	if err != nil {
		t.Fatalf("Get was failed: %v", err)
	}
	if got != "new" {
		t.Errorf("a new value was waited, but got '%s'", got)
	}

	if len(ds.segments) != 1 {
		t.Errorf("only one segment was waited after merge, but got %d", len(ds.segments))
	}
}

func TestDelete(t *testing.T) {
	dir := t.TempDir()

	ds, err := NewSegmentedDatastore(dir, testMaxSegmentSize)
	if err != nil {
		t.Fatalf("failed to create datastore: %v", err)
	}
	defer ds.Close()

	key := "key"
	value := "value"

	if err := ds.Put(key, value); err != nil {
		t.Fatalf("failed to write value: %v", err)
	}

	if err := ds.Delete(key); err != nil {
		t.Fatalf("failed to delete key: %v", err)
	}

	_, err = ds.Get(key)
	if err == nil {
		t.Fatalf("expected error, but key still exists")
	}

	expectedErr := "key not found: key"
	if err.Error() != expectedErr {
		t.Fatalf("expected error '%s', received: '%v'", expectedErr, err)
	}
}
