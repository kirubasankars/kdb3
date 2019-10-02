package main

import (
	mrand "math/rand"
	"time"
)

type SequenceGenarator struct {
	charSet []byte
	len     int

	current []int
	number  int
}

func NewSequenceGenarator(l int, seedNumber int, seedId string) *SequenceGenarator {
	seq := &SequenceGenarator{}
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

func (seq *SequenceGenarator) Next() (int, string) {

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
