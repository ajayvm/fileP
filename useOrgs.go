package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"time"

	"google.golang.org/protobuf/proto"
)

// this class is to get all organizations and unmarshal them for proto
// it will help to assess the memory and processing overheads of proto and the bolt serialization

func main() {
	orgIds := getOrgsFromCsv()
	st6 := time.Now()
	bytesInDb := getAllObj(orgIds)
	st7 := time.Now()

	// 10. Now unmarshal all
	for _, v := range bytesInDb {
		orgRec := Organization{}
		proto.Unmarshal(v, &orgRec)
	}
	st8 := time.Now()

	fmt.Println("time to get ", len(bytesInDb), " Orgs ", (st7.Sub(st6)),
		" time to unmarshall them ", (st8.Sub(st7)))
}

// get all orgs
func getOrgsFromCsv() []string {
	f, err := os.Open("OrgIds.csv")
	if err != nil {
		log.Fatal("Unable to read input file ", err)
	}
	defer f.Close()

	csvReader := csv.NewReader(f)
	records, err := csvReader.ReadAll()
	if err != nil {
		log.Fatal("Unable to parse file as CSV for ", err)
	}
	orgIds := make([]string, len(records))
	for i, v := range records {
		orgIds[i] = v[0]
	}
	return orgIds
}
