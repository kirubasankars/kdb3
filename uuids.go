package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math/big"
	"sync"
	"sync/atomic"
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
