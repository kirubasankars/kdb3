package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
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

func formatSeq(seqNumber int, seqID string) string {
	return fmt.Sprintf("%d-%s", seqNumber, seqID)
}

func formatDocString(id string, version int, deleted bool) string {
	if version != 0 {
		if deleted {
			return fmt.Sprintf(`{"_id" :"%s","_version":%d,"deleted":true}`, id, version)
		}
		return fmt.Sprintf(`{"_id":"%s","_version":%d}`, id, version)
	}
	if deleted {
		return fmt.Sprintf(`{"_id" :"%s","deleted":true}`, id)
	}
	return fmt.Sprintf(`{"_id":"%s"}`, id)
}
