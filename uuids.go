package main

import (
	"crypto/rand"
	"encoding/hex"
	"math/big"
)

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

var node = hex.EncodeToString([]byte(getMacAddr()))

func RandomUUID() string {
	return hex.EncodeToString(randomBytes(16))
}
