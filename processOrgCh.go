package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"google.golang.org/protobuf/proto"
)

// read json using decoder into a record and then send it over channel
// 3 go routines, viz. first on file read, second on processing and third on db save

// constants
const orgJsonFile = "org100.json" // "org2m.json" // "org100.json"
const replaceBoltDb = true

func main() {

	// 1. remove the existing bolt db if required
	if replaceBoltDb {
		err := os.Remove("orgbolt.db")
		if err != nil {
			panic(err)
		}
	}

	stStart := time.Now()
	// first read json as a single record
	input, err := os.Open(orgJsonFile)
	if err != nil {
		panic(err)
	}
	dec := json.NewDecoder(input)

	// read open bracket
	dec.Token()          // {
	dec.Token()          // org
	_, err = dec.Token() // [
	if err != nil {
		panic(err)
	}

	// Before processing the loop, initialize and remember to close db operations
	initBoltDb()
	recProcessed := 0

	for dec.More() {
		var org Organization
		err := dec.Decode(&org)
		if err != nil {
			panic(err)
		}
		k, b, err := process(&org)
		if err != nil {
			fmt.Println("error in converting", err)
		}
		if replaceBoltDb {
			err = batchSaveToDb(k, b)
			if err != nil {
				fmt.Println("error in saving", err)
			}
		}
		recProcessed++
	}
	closeBoltDb()
	stEnd := time.Now()
	fmt.Println(recProcessed, " records processed in ", (stEnd.Sub(stStart)))
}

// validate data, convert to protobuf and return byte array
func process(org *Organization) (string, []byte, error) {

	orgId := org.Org
	b, err := proto.Marshal(org)
	return orgId, b, err
}
