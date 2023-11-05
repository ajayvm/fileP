package main

// this class has generic trials - e.g. to check sorting of the slices

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
)

func main() {
	kvMap := make(map[string]string)
	byteSlice := make([][]byte, 0, 100)

	for i := 0; i < 100; i++ {
		iStr := strconv.Itoa(i)
		kvMap["k"+iStr] = "v" + iStr
		keyB := []byte("k" + iStr)
		byteSlice = append(byteSlice, keyB)
	}
	fmt.Println(byteSlice)
	sort.Slice(byteSlice, func(i, j int) bool { return bytes.Compare(byteSlice[i], byteSlice[j]) < 0 })
	fmt.Println("after sorting", (byteSlice))
}
