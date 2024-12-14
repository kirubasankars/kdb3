package main

import (
	"encoding/hex"
	mrand "math/rand"
	"sync"
)

type ChangeSequenceGenarator struct {
	current int
}

func NewChangeSequenceGenarator(current int) *ChangeSequenceGenarator {
	return &ChangeSequenceGenarator{current: current}
}

func (seq *ChangeSequenceGenarator) Next() int {
	seq.current = seq.current + 1
	return seq.current
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
			var newCurrent []int
			for i := 0; i < seq.len; i++ {
				newCurrent = append(newCurrent, mrand.Intn(36))
			}
			seq.current = newCurrent

			seq.number = 1
			seq.prefix = hex.EncodeToString(randomBytes(13))
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
