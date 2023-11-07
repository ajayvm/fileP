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

const useBolt = false

func main() {
	orgIds := getOrgsFromCsv()
	// 10. Now unmarshal all
	if useBolt {
		getFromBolt(orgIds)
	} else {
		getFromProto(orgIds)
	}
}

func getFromProto(orgIds []string) {
	st6 := time.Now()
	b, err := os.ReadFile("org2m.proto")
	if err != nil {
		log.Fatal("Unable to read input file ", err)
	}
	orgList := OrgList{}
	proto.Unmarshal(b, &orgList)
	st7 := time.Now()

	orgMap := make(map[string]*Organization)
	// convert to maps
	for _, v := range orgList.Org {
		orgMap[v.Org] = v
	}
	st8 := time.Now()

	// now search for all passed ids and return orgs that match
	for _, v := range orgIds {
		if _, found := orgMap[v]; !found {
			fmt.Println("didnt find id ", v)
		}
	}
	st9 := time.Now()

	fmt.Println("Time to unmarshal ", (st7.Sub(st6)), " time to convert to map ", (st8.Sub(st7)),
		" time to search passed ", len(orgIds), " ids ", (st9.Sub(st8)))
}

func getFromBolt(orgIds []string) {
	st6 := time.Now()
	bytesInDb := getAllObj(orgIds)
	st7 := time.Now()

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
