package main

import (
	"errors"
	"log"
	"os"
	"strings"
	"testing"
)

const (
	mp   = "./test/"
	tf   = "test_file.txt"
	data = "somedata\n"
)

var (
	ErrFileNotFound = errors.New("file not found in listing")
)

/* tests of ss3fs */
func TestCreateFileRemove(t *testing.T) {
	_, err := os.Create(mp + tf)
	if err != nil {
		t.Errorf("File wasn't created, error: %v\n", err)
		return
	}

	ok := os.Remove(mp + tf)
	if ok != nil {
		t.Errorf("Can't remove file %s, error: %v\n", mp+tf, ok)
		return
	}
}

func TestCreateFileWR(t *testing.T) {
	file, err := os.Create(mp + tf)
	if err != nil {
		t.Errorf("File wasn't created, error: %v\n", err)
		return
	}
	test_str := []byte(data)
	n, ok := file.Write(test_str)
	if ok != nil || n != len(test_str) {
		t.Errorf("Failed write into file, error: %v\n", err)
	}
	res := make([]byte, n)
	n, ok = file.ReadAt(res, 0)
	if ok != nil || n != len(test_str) || strings.Compare(string(res), data) != 0 {
		t.Errorf("Failed read file, error: %v\n", err)
	}
	err = os.Remove(mp + tf)
	if err != nil {
		t.Errorf("Can't remove file, error: %v\n", err)
		return
	}
}

func TestCreateFileOpen(t *testing.T) {
	file, err := os.Create(mp + tf)
	if err != nil {
		t.Errorf("File wasn't created, error: %v\n", err)
		return
	}
	err = file.Close()
	if err != nil {
		t.Errorf("Can't close file, error: %v\n", err)
		return
	}
	file, err = os.Open(mp + tf)
	if err != nil {
		t.Errorf("Can't open file, error: %v\n", err)
		return
	}
	err = file.Close()
	if err != nil {
		t.Errorf("Can't close file, error: %v\n", err)
		return
	}
	err = os.Remove(mp + tf)
	if err != nil {
		t.Errorf("Can't remove file, error: %v\n", err)
		return
	}
}

func TestReadDir(t *testing.T) {
	file, err := os.Create(mp + tf)
	if err != nil {
		t.Errorf("File wasn't created, error: %v\n", err)
		return
	}
	err = file.Close()
	if err != nil {
		t.Errorf("Can't close file, error: %v\n", err)
		return
	}
	dEntry, err := os.ReadDir(mp)
	if err != nil {
		t.Errorf("Can't list objects, error: %v\n", err)
		return
	}
	err = ErrFileNotFound
	for _, f := range dEntry {
		log.Println(f.Name())
		if strings.Compare(f.Name(), tf) == 0 {
			err = nil
		}
	}
	if err != nil {
		t.Errorf("Can't list objects in %v, error: %v\n", dEntry, err)
		return
	}
	err = os.Remove(mp + tf)
	if err != nil {
		t.Errorf("Can't remove file, error: %v\n", err)
		return
	}
}
