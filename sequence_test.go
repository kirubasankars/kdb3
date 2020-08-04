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
	a := NewChangeSequenceGenarator(2, "zz")
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

func TestSequenceUUIDGenaratorNext(t *testing.T) {
	seq := NewSequenceUUIDGenarator()
	id1 := seq.Next()

	for i := 0; i < 1000; i++ {
		n := seq.Next()
		if id1 > n {
			t.Errorf("failed to have next seq id")
		}
		id1 = n
	}
}

func TestSequenceUUIDGenaratorNext1(t *testing.T) {
	seq := NewSequenceUUIDGenarator()
	id1 := seq.Next()
	seq.number = 16773118
	id2 := seq.Next()
	id3 := seq.Next()

	if id1[0:len(id1)-6] != id2[0:len(id2)-6] {
		t.Errorf("expected has same value")
	}

	if id2[0:len(id2)-6] == id3[0:len(id3)-6] {
		t.Errorf("expected has different value")
	}
}

func TestSequenceUUIDGenaratorNext2(t *testing.T) {
	seq := NewSequenceUUIDGenarator()
	id1 := seq.Next()

	var newCurrent []int
	for i := 0; i < 6; i++ {
		newCurrent = append(newCurrent, 35)
	}
	seq.current = newCurrent

	id2 := seq.Next()

	if id1[0:len(id1)-6] == id2[0:len(id2)-6] {
		t.Errorf("expected has different value")
	}

	if id1[len(id1)-6:] == id2[len(id2)-6:] {
		t.Errorf("expected has different value")
	}
}
