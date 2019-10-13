package main

import (
	"encoding/hex"
	mrand "math/rand"
	"sync"
	"time"
)

type ChangeSequenceGenarator struct {
	charSet []byte
	len     int

	current   []int
	number    int
	endString []int
}

func NewChangeSequenceGenarator(l int, seedNumber int, seedId string) *ChangeSequenceGenarator {
	seq := &ChangeSequenceGenarator{}
	seq.charSet = []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz")
	seq.len = l

	mrand.Seed(time.Now().UnixNano())
	if seedId == "" {
		for i := 0; i < l; i++ {
			seq.current = append(seq.current, mrand.Intn(63))
		}
	} else {
		seq.number = seedNumber
		if l != len(seedId) {
			panic("seed value has to match len")
		}

		for _, x := range []byte(seedId) {
			for j, y := range seq.charSet {
				if x == y {
					seq.current = append(seq.current, j)
				}
			}
		}
	}

	return seq
}

func (seq *ChangeSequenceGenarator) Next() (int, string) {

	reachedEnd := false
	for i := seq.len - 1; i >= 0; i-- {
		t := seq.current[i] + 1
		if t == 63 {
			reachedEnd = true
			t = 0
		} else {
			reachedEnd = false
		}

		seq.current[i] = t

		if i == 0 && reachedEnd {
			return 0, ""
		}

		if !reachedEnd {
			break
		}
	}

	v := []byte("")
	for i := 0; i < seq.len; i++ {
		v = append(v, seq.charSet[seq.current[i]])
	}

	seq.number++

	if seq.number >= 16773120 {
		seq.number = 1
	}

	return seq.number, string(v)
}

type SequenceUUIDGenarator struct {
	charSet []byte
	len     int

	current   []int
	number    int
	endString []int
	prefix    string
	syncLock  sync.Mutex
}

func NewSequenceUUIDGenarator() *SequenceUUIDGenarator {
	seq := &SequenceUUIDGenarator{}
	seq.charSet = []byte("0123456789abcdefghijklmnopqrstuvwxyz")
	seq.len = 6
	mrand.Seed(1)
	for i := 0; i < seq.len; i++ {
		seq.current = append(seq.current, mrand.Intn(36))
	}
	seq.prefix = hex.EncodeToString(randomBytes(13))
	return seq
}

func (seq *SequenceUUIDGenarator) Next() string {
	seq.syncLock.Lock()

	reachedEnd := false
	for i := seq.len - 1; i >= 0; i-- {
		t := seq.current[i] + 1
		if t == 36 {
			reachedEnd = true
			t = 0
		} else {
			reachedEnd = false
		}

		seq.current[i] = t

		if i == 0 && reachedEnd {
			return ""
		}

		if !reachedEnd {
			break
		}
	}

	v := []byte("")
	for i := 0; i < seq.len; i++ {
		v = append(v, seq.charSet[seq.current[i]])
	}

	seq.number++

	if seq.number >= 16773120 {
		seq.number = 1
		seq.prefix = hex.EncodeToString(randomBytes(13))
	}

	seq.syncLock.Unlock()

	return string(seq.prefix) + string(v)
}
