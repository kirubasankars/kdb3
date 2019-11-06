package main

import (
	"testing"
)

func TestNewSequence(t *testing.T) {
	seqGen := NewChangeSequenceGenarator(136, "")
	currentSeqID := seqGen.Next()
	i := 0
	for {
		nextSeqID := seqGen.Next()

		if currentSeqID < nextSeqID {
			currentSeqID = nextSeqID
		} else {
			t.Error("seq order missing")
		}

		if i >= 1000000 {
			break
		}
		i++
	}
}

func TestNewSequenceNoMatchLen(t *testing.T) {
	assertPanic(t, func() { NewChangeSequenceGenarator(2, "1") })
}

func TestNewSequenceEndfoWorld(t *testing.T) {
	a := NewChangeSequenceGenarator(1, "z")
	assertPanic(t, func() { a.Next() })
}

func assertPanic(t *testing.T, f func()) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The code did not panic")
		}
	}()
	f()
}
