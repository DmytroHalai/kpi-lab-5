package datastore

import (
	"errors"
	"fmt"
	"path/filepath"
)

type SegmentedDatastore struct {
  dir            string
  segments       []*Db
  maxSegmentSize int64
}

func NewSegmentedDatastore(dir string) (*SegmentedDatastore, error) {
  ds := &SegmentedDatastore{
    dir: dir,
  }

if err := ds.createNewSegment(); err != nil {
    return nil, err
  }
  return ds, nil
}

func (ds *SegmentedDatastore) createNewSegment() error {
  segmentName := fmt.Sprintf("segment-%d", len(ds.segments))
  path := filepath.Join(ds.dir, segmentName)

  db, err := Open(path)
  if err != nil {
    return err
  }
  ds.segments = append(ds.segments, db)
  return nil
}

func (ds *SegmentedDatastore) Put(key, value string) error {
  if len(ds.segments) == 0 {
    if err := ds.createNewSegment(); err != nil {
      return err
    }
  }

  active := ds.segments[len(ds.segments)-1]

  size, err := active.Size()
  if err != nil {
    if err := active.Close(); err != nil {
      return err
    }
    if err := ds.createNewSegment(); err != nil {
      return err
    }
    active = ds.segments[len(ds.segments)-1]
  }

  if size >= ds.maxSegmentSize {
    if err := active.Close(); err != nil {
      return err
    }
    if err := ds.createNewSegment(); err != nil {
      return err
    }
    active = ds.segments[len(ds.segments)-1]
  }

  return active.Put(key, value)
}

func (ds *SegmentedDatastore) Get(key string) (string, error) {
  for i := len(ds.segments) - 1; i >= 0; i-- {
    value, err := ds.segments[i].Get(key)
    if err == nil {
      return value, nil
    }
    if !errors.Is(err, ErrNotFound) {
      return "", err
    }
  }
  return "", fmt.Errorf("key not found: %s", key)
}

func (ds *SegmentedDatastore) Close() error {
  for _, segment := range ds.segments {
    if err := segment.Close(); err != nil {
      return err
    }
  }
  return nil
}
