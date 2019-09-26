package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math/big"
	mrand "math/rand"
	"sync"
	"sync/atomic"
	"time"
)

var syncLock sync.Mutex
var prefix []byte
var maxSeq uint32 = 16773121

func randomBytes(n int) []byte {
	bytes := make([]byte, n)
	_, _ = rand.Read(bytes)
	return bytes
}

func randNumber() int32 {
	randvalue, err := rand.Int(rand.Reader, big.NewInt(4094))
	if err != nil {
		panic(err)
	}
	return int32(randvalue.Int64())
}

func SequentialUUID() string {
	value := atomic.AddUint32(&maxSeq, 1)
	if maxSeq >= 16773120 {
		syncLock.Lock()
		if maxSeq >= 16773120 {
			maxSeq = 1
			prefix = randomBytes(13)
		}
		syncLock.Unlock()
		return SequentialUUID()
	}
	return hex.EncodeToString(prefix) + fmt.Sprintf("%06x", value)
}

var node = hex.EncodeToString([]byte(getMacAddr()))

func RandomUUID() string {
	return hex.EncodeToString(randomBytes(16))
}

func GetUUIDs(count int) []string {
	uuidType := "sequential"
	uuids := make([]string, count)
	if uuidType == "sequential" {
		for i := 0; i < count; i++ {
			uuids[i] = SequentialUUID()
		}
	}
	if uuidType == "random" {
		for i := 0; i < count; i++ {
			uuids[i] = RandomUUID()
		}
	}

	return uuids
}

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
