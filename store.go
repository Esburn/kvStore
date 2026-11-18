package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var errBadChars = errors.New("key/value cannot contain tab or newline")

type Store struct {
	mu        sync.RWMutex
	dirty     atomic.Bool
	data      map[string]string
	log       *os.File
	path      string
	mode      SyncType
	done      chan struct{}
	compChan  chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
	closeErr  error
	lines     int
}

func Open(path string, mode SyncType) (*Store, error) {
	store := &Store{data: make(map[string]string), mode: mode, done: make(chan struct{}), compChan: make(chan struct{})}

	err := replay(path, store)
	if err != nil {
		return nil, err
	}
	fd, err := os.OpenFile(filepath.Join(path, "data.log"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	store.log = fd
	store.path = fd.Name()
	if mode == SyncOnTimer {
		store.wg.Add(1)
		go store.syncLoop(time.Second)
	}
	store.wg.Add(1)
	go store.autoCompact()
	return store, nil
}

func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		if s.done != nil {
			close(s.done)
			s.wg.Wait()
		}
		syncErr := s.log.Sync()
		closErr := s.log.Close()
		s.closeErr = errors.Join(syncErr, closErr)
	})
	return s.closeErr
}

func (s *Store) Compact() error {
	s.mu.Lock()
	path := s.path
	tmpPath := path + ".tmp"
	defer s.mu.Unlock()
	tmpFd, err := os.Create(tmpPath)
	if err != nil {
		return err
	}

	for k, v := range s.data {
		_, err := fmt.Fprintf(tmpFd, "set\t%s\t%s\n", k, v)
		if err != nil {
			if clErr := tmpFd.Close(); clErr != nil {
				return errors.Join(err, clErr)
			}
			if rmErr := os.Remove(tmpPath); rmErr != nil {
				return errors.Join(err, rmErr)
			}
			return err
		}
	}
	if err := tmpFd.Sync(); err != nil {
		if clErr := tmpFd.Close(); clErr != nil {
			return errors.Join(err, clErr)
		}
		if rmErr := os.Remove(tmpPath); rmErr != nil {
			return errors.Join(err, rmErr)
		}
		return err
	}
	if err := tmpFd.Close(); err != nil {
		return err
	}

	closeErr := s.log.Close()
	var renameErr error
	if closeErr == nil {
		renameErr = os.Rename(tmpFd.Name(), path)
		if renameErr == nil {
			s.lines = len(s.data)
		}
	}
	aFd, openErr := os.OpenFile(s.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if openErr == nil {
		s.log = aFd
	}

	return errors.Join(closeErr, renameErr, openErr)
}

func (s *Store) backup() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.log.Sync(); err != nil {
		return err
	}
	// if err := copyData(fmt.Sprintf("%s-%s.bk", s.path, time.Now().Format("20060102-150405")), s.path); err != nil {
	// 	return err
	// }
	// return nil
	return copyData(fmt.Sprintf("%s-%s.bk", s.path, time.Now().Format("20060102-150405")), s.path)
}

func (s *Store) syncLoop(every time.Duration) {
	defer s.wg.Done()
	t := time.NewTicker(every)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			if !s.dirty.Swap(false) {
				continue
			}
			s.mu.Lock()
			if err := s.log.Sync(); err != nil {
				log.Printf("Sync err: %s", err)
			}
			s.mu.Unlock()
		case <-s.done:
			return
		}
	}
}

func (s *Store) needsCompact() bool {
	return s.lines > 1000 && s.lines > 2*len(s.data)
}

func (s *Store) autoCompact() {
	defer s.wg.Done()

	for {
		select {
		case <-s.compChan:
			if err := s.Compact(); err != nil {
				log.Printf("AutoCompact err: %s", err)
			}

		case <-s.done:
			return
		}
	}
}

// func replay(path string, data map[string]string) error {
func replay(path string, store *Store) error {
	fName := filepath.Join(path, "data.log")
	fd, err := os.Open(fName)

	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	defer fd.Close()

	r := bufio.NewReader(fd)
	badLine := 0
	currLine := 0
	offset := 0

	for {
		raw, err := r.ReadBytes('\n')
		if len(raw) > 0 {
			line := strings.TrimRight(string(raw), "\r\n")
			currLine++

			if badLine != 0 {
				return fmt.Errorf("data.log: corrupt line %d", badLine)
			}

			parts := strings.Split(line, "\t")
			lLen := len(parts)

			switch parts[0] {
			case "set":
				if lLen != 3 {
					badLine = currLine
					continue
				}
				store.data[parts[1]] = parts[2]

			case "del":
				if lLen != 2 {
					badLine = currLine
					continue
				}
				delete(store.data, parts[1])

			default:
				badLine = currLine
				continue
			}
			offset += len(raw)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}

	store.lines = currLine

	if badLine != 0 {
		fd.Close()
		store.lines--

		bckName := fName + ".corrupt-" + time.Now().Format("20060102-150405")
		if err := copyData(bckName, fName); err != nil {
			return err
		}

		if err := os.Truncate(fName, int64(offset)); err != nil {
			return err
		}

		log.Printf("data.log: torn write at line %d, truncated", badLine)

	}
	return nil
}

func copyData(dst, src string) (err error) {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := out.Close(); cerr != nil && err == nil {

			err = cerr
		}
	}()

	if _, err := io.Copy(out, in); err != nil {
		if rmErr := os.Remove(out.Name()); rmErr != nil {
			return errors.Join(err, rmErr)
		}
		return err
	}
	return out.Sync()
}

func (s *Store) Set(k, v string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if strings.ContainsAny(k, "\t\n") || strings.ContainsAny(v, "\t\n") {
		return errBadChars
	}

	_, err := fmt.Fprintf(s.log, "set\t%s\t%s\n", k, v)
	if err != nil {
		return err
	}

	s.dirty.Store(true)

	if s.mode == SyncOnWrite {
		if err := s.log.Sync(); err != nil {
			return err
		}
	}

	s.data[k] = v
	s.lines++
	if s.needsCompact() {
		select {
		case s.compChan <- struct{}{}:
		default:
		}
	}

	return nil
}

func (s *Store) Get(k string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	val, ok := s.data[k]
	return val, ok

}

func (s *Store) Delete(k string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if strings.ContainsAny(k, "\t\n") {
		return errBadChars
	}
	_, err := fmt.Fprintf(s.log, "del\t%s\n", k)
	if err != nil {
		return err
	}

	s.dirty.Store(true)

	if s.mode == SyncOnWrite {
		if err := s.log.Sync(); err != nil {
			return err
		}
	}

	s.lines++
	delete(s.data, k)
	if s.needsCompact() {
		select {
		case s.compChan <- struct{}{}:
		default:
		}
	}
	return nil
}
