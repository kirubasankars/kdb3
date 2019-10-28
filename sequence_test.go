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
