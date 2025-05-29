package datastore

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type Manifest struct {
	Segments    []string `json:"segments"`
	ActiveIndex int      `json:"active_index"`
}

func loadManifest(dir string) (*Manifest, error) {
	path := filepath.Join(dir, "manifest.json")
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &Manifest{Segments: []string{}, ActiveIndex: -1}, nil
		}
		return nil, fmt.Errorf("failed to open manifest: %w", err)
	}
	defer f.Close()

	var manifest Manifest
	if err := json.NewDecoder(f).Decode(&manifest); err != nil {
		return nil, fmt.Errorf("failed to decode manifest: %w", err)
	}
	return &manifest, nil
}

func saveManifest(dir string, manifest *Manifest) error {
	path := filepath.Join(dir, "manifest.json")
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	encoder := json.NewEncoder(f)
	encoder.SetIndent("", "  ")
	return encoder.Encode(manifest)
}
