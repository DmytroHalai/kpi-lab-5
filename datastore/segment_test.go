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
		t.Fatalf("не вдалося створити каталог testdata: %v", err)
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
			t.Fatalf("помилка при Put(%s): %v", key, err)
		}
	}

	if len(ds.segments) < 2 {
		t.Errorf("очікувалося принаймні 2 сегменти, отримано %d", len(ds.segments))
	}

	for i := 0; i < totalEntries; i++ {
		key := fmt.Sprintf("key%d", i)
		expected := fmt.Sprintf("value%d", i)
		value, err := ds.Get(key)
		if err != nil {
			t.Errorf("Get(%s): %v", key, err)
		}
		if value != expected {
			t.Errorf("%s: очікувалося %s, отримано %s", key, expected, value)
		}
	}
}

func TestMergeSegments(t *testing.T) {
	dir := "testdata_merge"
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	ds, err := NewSegmentedDatastore(dir, testMaxSegmentSize)
	if err != nil {
		t.Fatalf("не вдалося створити datastore: %v", err)
	}
	defer ds.Close()

	key := "key"

	if err := ds.Put(key, "old"); err != nil {
		t.Fatalf("не вдалося записати старе значення: %v", err)
	}

	if err := ds.createNewSegment(); err != nil {
		t.Fatalf("не вдалося створити новий сегмент: %v", err)
	}

	if err := ds.Put(key, "new"); err != nil {
		t.Fatalf("не вдалося записати нове значення: %v", err)
	}

	for i, seg := range ds.segments {
		_, err := seg.ReadAll()
		if err != nil {
			t.Errorf("сегмент %d (%s): помилка читання: %v", i, seg.filename, err)
		}
	}

	if err := ds.Merge(); err != nil {
		t.Fatalf("злиття не вдалося: %v", err)
	}

	got, err := ds.Get(key)
	if err != nil {
		t.Fatalf("Get не вдався: %v", err)
	}
	if got != "new" {
		t.Errorf("очікувалося значення 'new', отримано '%s'", got)
	}

	if len(ds.segments) != 1 {
		t.Errorf("очікувався 1 сегмент після злиття, отримано %d", len(ds.segments))
	}
}
