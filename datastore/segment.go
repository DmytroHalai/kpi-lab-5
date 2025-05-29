package datastore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

type SegmentedDatastore struct {
  dir            string
  segments       []*Db
  maxSegmentSize int64
}

func NewSegmentedDatastore(dir string, maxSegmentSize int64) (*SegmentedDatastore, error) {
  ds := &SegmentedDatastore{
    dir:            dir,
    maxSegmentSize: maxSegmentSize,
  }

  manifest, err := loadManifest(ds.dir)
  if err != nil {
    return nil, err
  }

  for _, segFile := range manifest.Segments {
      path := filepath.Join(dir, segFile)
      fmt.Printf("Opening segment: %q\n", path)  
      db, err := Open(path)
      if err != nil {
          return nil, fmt.Errorf("failed to open segment %q: %w", path, err)
      }
      ds.segments = append(ds.segments, db)
}


  if len(ds.segments) == 0 {
    if err := ds.createNewSegment(); err != nil {
      return nil, err
    }
    manifest.Segments = []string{fmt.Sprintf("segment-%d.db", len(ds.segments)-1)}
    manifest.ActiveIndex = 0
    if err := saveManifest(ds.dir, manifest); err != nil {
      return nil, err
    }
  }

  return ds, nil
}

func (ds *SegmentedDatastore) createNewSegment() error {
    segmentName := fmt.Sprintf("segment-%d.db", len(ds.segments))
    path := filepath.Join(ds.dir, segmentName)

    if err := os.MkdirAll(ds.dir, 0755); err != nil {
        return fmt.Errorf("failed to create directory %s: %w", ds.dir, err)
    }

    db, err := Open(path)
    if err != nil {
        return fmt.Errorf("failed to open segment %s: %w", path, err)
    }
    ds.segments = append(ds.segments, db)

    manifest, err := loadManifest(ds.dir)
    if err != nil {
        db.Close()
        return err
    }

    manifest.Segments = append(manifest.Segments, segmentName)
    manifest.ActiveIndex = len(manifest.Segments) - 1

    return saveManifest(ds.dir, manifest)
}


func (ds *SegmentedDatastore) Merge() error {
    latest := make(map[string]string)

    for _, segment := range ds.segments {
        entries, err := segment.ReadAll()
        if err != nil {
            return fmt.Errorf("failed to read segment %s: %w", segment.filename, err)
        }
        fmt.Printf("Segment %s: read %d records\n", segment.filename, len(entries))
        for _, e := range entries {
            latest[e.Key] = e.Value
            fmt.Printf("Read key: %s, value: %s\n", e.Key, e.Value)
        }
    }

    if len(latest) == 0 {
        fmt.Println("Warning: latest is empty, no data to merge")
    }

    tmpSegmentName := fmt.Sprintf("tmp-segment-%d.tmp", len(ds.segments))
    tmpPath := filepath.Join(ds.dir, tmpSegmentName)

    if err := os.MkdirAll(ds.dir, 0755); err != nil {
        return fmt.Errorf("failed to create directory %s: %w", ds.dir, err)
    }

    tmpDb, err := Open(tmpPath)
    if err != nil {
        return fmt.Errorf("failed to open temporary segment %s: %w", tmpPath, err)
    }

    for key, value := range latest {
        fmt.Printf("Writing to tmpDb key: %s, value: %s\n", key, value)
        if err := tmpDb.Put(key, value); err != nil {
            tmpDb.Close()
            os.RemoveAll(tmpPath)
            return err
        }
    }

    if err := tmpDb.Close(); err != nil {
        return fmt.Errorf("failed to close tmpDb before renaming: %w", err)
    }

    for _, seg := range ds.segments {
        seg.Close()
        os.Remove(seg.filename)
    }

    finalSegmentName := fmt.Sprintf("segment-%d.db", len(ds.segments))
    finalPath := filepath.Join(ds.dir, finalSegmentName)

    if err := os.Rename(tmpPath, finalPath); err != nil {
        return fmt.Errorf("failed to rename %s to %s: %w", tmpPath, finalPath, err)
    }

    newDb, err := Open(finalPath)
    if err != nil {
        return err
    }

    ds.segments = []*Db{newDb}

    manifest := &Manifest{
        Segments:    []string{finalSegmentName},
        ActiveIndex: 0,
    }

    return saveManifest(ds.dir, manifest)
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
