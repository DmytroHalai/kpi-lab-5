package datastore

import (
	"fmt"
	"os"
	"testing"
)

const testMaxSegmentSize = 1 << 10 

func TestSegmentsCreation(t *testing.T) {
  dir := "testdata"
  os.RemoveAll(dir)
  if err := os.MkdirAll(dir, 0755); err != nil {
    t.Fatalf("failed to create testdata directory: %v", err)
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
      t.Fatalf("error on Put(%s): %v", key, err)
    }
  }

  if len(ds.segments) < 2 {
    t.Errorf("expected at least 2 segments, go %d", len(ds.segments))
  }

  for i := 0; i < totalEntries; i++ {
    key := fmt.Sprintf("key%d", i)
    expected := fmt.Sprintf("value%d", i)
    value, err := ds.Get(key)
    if err != nil {
      t.Errorf("Get(%s): %v", key, err)
    }
    if value != expected {
      t.Errorf("%s: expected %s, got %s", key, expected, value)
    }
  }
}


func TestMergeSegments(t *testing.T) {
  dir := "testdata_merge"
  os.RemoveAll(dir)
  defer os.RemoveAll(dir)

  ds, err := NewSegmentedDatastore(dir, testMaxSegmentSize)
  if err != nil {
    t.Fatalf("failed to create datastore: %v", err)
  }
  defer ds.Close()

  key := "key"

  if err := ds.Put(key, "old"); err != nil {
    t.Fatalf("failed to write old value: %v", err)
  }

  if err := ds.createNewSegment(); err != nil {
    t.Fatalf("failed to write new value: %v", err)
  }

  if err := ds.Put(key, "new"); err != nil {
    t.Fatalf("failed to create new segment: %v", err)
  }

  for i, seg := range ds.segments {
    _, err := seg.ReadAll()
    if err != nil {
      t.Errorf("segment %d (%s): read error: %v", i, seg.filename, err)
    }
  }

  if err := ds.Merge(); err != nil {
    t.Fatalf("merge failed: %v", err)
  }

  got, err := ds.Get(key)
  if err != nil {
    t.Fatalf("Get failed: %v", err)
  }
  if got != "new" {
    t.Errorf("expected value 'new', got '%s'", got)
  }

  if len(ds.segments) != 1 {
    t.Errorf("expected 1 segment after merge, got %d", len(ds.segments))
  }
}
