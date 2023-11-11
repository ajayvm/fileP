package main

import (
	"fmt"
	"runtime"
	"strconv"
	"time"

	"github.com/segmentio/fasthash/fnv1a"
)

// this class has generic trials - e.g. to check sorting of the slices

func main() {

	// defer profile.Start(profile.MemProfile, profile.ProfilePath(".")).Stop()

	// sorting the byte slice
	// kvMap := make(map[string]string)
	// 	byteSlice := make([][]byte, 0, 100)

	distMap := make(map[uint64]int)

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Println("initial:", m.Alloc)

	st1 := time.Now()
	for i := 0; i < 10000000; i++ {
		iStr := strconv.Itoa(i)
		key := "k" + iStr
		_ = key
		// kvMap[key] = "v" + iStr
		// keyB := []byte("k" + iStr)
		// byteSlice = append(byteSlice, keyB)

		shardNo := getShardNoForStr(key, 32)
		distMap[shardNo]++
	}
	sttaken := time.Since(st1)
	runtime.ReadMemStats(&m)
	fmt.Println("After Hash post GC:", m.Alloc)
	runtime.GC()
	runtime.ReadMemStats(&m)
	fmt.Println("After Hash post GC:", m.Alloc)
	fmt.Println("the hash function separation is ", distMap, "\n time ", sttaken)

	// Checking the sorting
	// fmt.Println(byteSlice)
	// sort.Slice(byteSlice, func(i, j int) bool { return bytes.Compare(byteSlice[i], byteSlice[j]) < 0 })
	// fmt.Println("after sorting", (byteSlice))

}

// This function has an accurate hash with very low perf and memory impact. 10M in 200-250 ms
func getShardNoForStr(inp string, shards uint64) uint64 {
	h := fnv1a.HashString64(inp)
	return h % shards
}
