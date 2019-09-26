package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"hash/fnv"
	"net"
)

var algorithm = fnv.New32()

func Signature(text []byte) string {
	return hex.EncodeToString(algorithm.Sum(text))
}

func JSON(obj interface{}) []byte {
	jsonBytes, _ := json.Marshal(obj)
	return jsonBytes
}

func getMacAddr() (addr string) {
	interfaces, err := net.Interfaces()
	if err == nil {
		for _, i := range interfaces {
			if i.Flags&net.FlagUp != 0 && bytes.Compare(i.HardwareAddr, nil) != 0 {
				// Don't use random as we have a real address
				addr = i.HardwareAddr.String()
				break
			}
		}
	}
	return
}
