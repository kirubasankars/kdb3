package main

import (
	"testing"
)

func TestNewSequence(t *testing.T) {
	seqGen := NewSequenceGenarator(136, 0, "")
	currentSeqNumber, currentSeqID := seqGen.Next()
	i := 0
	for {
		nextSeqNumber, nextSeqID := seqGen.Next()

		if currentSeqNumber < nextSeqNumber && currentSeqID < nextSeqID {
			currentSeqNumber = nextSeqNumber
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
