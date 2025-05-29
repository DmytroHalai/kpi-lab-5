package datastore

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	outFileName      = "current-data"
	MAX_SEGMENT_SIZE = 1 << 10
)

var ErrNotFound = fmt.Errorf("record does not exist")

type hashIndex map[string]int64

type Db struct {
	out       *os.File
	outOffset int64
	filename  string
	index     hashIndex

	mu       sync.RWMutex
	writeCh  chan entry
	wg       sync.WaitGroup
	stopOnce sync.Once
}

type Entry struct {
	Key   string
	Value string
}

func (db *Db) ReadAll() ([]Entry, error) {
	file, err := os.Open(db.filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var entries []Entry
	reader := bufio.NewReader(file)
	for {
		var record entry
		n, err := record.DecodeFromReader(reader)
		if err != nil {
			if errors.Is(err, io.EOF) && n == 0 {
				break
			}
			return nil, fmt.Errorf("помилка при декодуванні запису: %w", err)
		}
		entries = append(entries, Entry{Key: record.key, Value: record.value})
	}
	return entries, nil
}

func Open(dir string) (*Db, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("не вдалося створити каталог %s: %w", dir, err)
	}
	outputPath := filepath.Join(dir, outFileName)
	f, err := os.OpenFile(outputPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	db := &Db{
		out:      f,
		filename: outputPath,
		index:    make(hashIndex),
		writeCh:  make(chan entry, 128),
	}
	err = db.recover()
	if err != nil && err != io.EOF {
		return nil, err
	}
	db.wg.Add(1)
	go db.writeLoop()
	return db, nil
}

func (db *Db) recover() error {
	f, err := os.Open(db.out.Name())
	if err != nil {
		return err
	}
	defer f.Close()
	in := bufio.NewReader(f)
	for err == nil {
		var (
			record entry
			n      int
		)
		n, err = record.DecodeFromReader(in)
		if errors.Is(err, io.EOF) {
			if n != 0 {
				return fmt.Errorf("corrupted file")
			}
			break
		}
		db.index[record.key] = db.outOffset
		db.outOffset += int64(n)
	}
	return err
}

func (db *Db) writeLoop() {
	defer db.wg.Done()
	for e := range db.writeCh {
		data := e.Encode()
		n, err := db.out.Write(data)
		if err != nil {
			fmt.Fprintf(os.Stderr, "write error: %v\n", err)
			continue
		}
		db.mu.Lock()
		db.index[e.key] = db.outOffset
		db.outOffset += int64(n)
		db.mu.Unlock()
	}
}

func (db *Db) Close() error {
	return db.out.Close()
}

func (db *Db) Get(key string) (string, error) {
	db.mu.RLock()
	position, ok := db.index[key]
	db.mu.RUnlock()
	if !ok {
		return "", ErrNotFound
	}
	file, err := os.Open(db.out.Name())
	if err != nil {
		return "", err
	}
	defer file.Close()
	_, err = file.Seek(position, 0)
	if err != nil {
		return "", err
	}
	var record entry
	if _, err = record.DecodeFromReader(bufio.NewReader(file)); err != nil {
		return "", err
	}
	return record.value, nil
}

func (db *Db) Put(key, value string) error {
	db.writeCh <- entry{key: key, value: value}
	return nil
}

func (db *Db) Size() (int64, error) {
	info, err := db.out.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}
