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
const useMap = false

var orgnMap = make(map[string]*Organization)

func main() {
	orgIds := getOrgsFromCsv()
	// 10. Now unmarshal all two times to see effect of caching
	if useBolt {
		getFromBolt(orgIds)
		getFromBolt(orgIds)
	} else {
		getFromProto(orgIds)
		getFromProto(orgIds)
	}
}

func getFromProto(orgIds []string) {
	st6 := time.Now()
	if useMap {
		loadProtoFromMap()
	} else {
		loadProtoFromList()
	}
	st7 := time.Now()

	// now search for all passed ids and return orgs that match
	foundCtr := 0
	notFoundCtr := 0

	for _, v := range orgIds {
		if _, found := orgnMap[v]; found {
			foundCtr++
		} else {
			notFoundCtr++
		}
	}
	st8 := time.Now()

	fmt.Println("use Slice not Map:", useMap, "Time to unmarshal ", (st7.Sub(st6)),
		" report F NF", foundCtr, notFoundCtr,
		" time to search ", len(orgIds), " ids ", (st8.Sub(st7)))
}

func loadProtoFromMap() {
	if len(orgnMap) == 0 {
		b, err := os.ReadFile("datafiles/org2mMap.proto")
		if err != nil {
			log.Fatal("Unable to read input file ", err)
		}
		orgMap := OrgMap{}
		proto.Unmarshal(b, &orgMap)
		orgnMap = orgMap.OrgM
	}
}

func loadProtoFromList() {
	if len(orgnMap) == 0 {
		b, err := os.ReadFile("datafiles/org2mList.proto")
		if err != nil {
			log.Fatal("Unable to read input file ", err)
		}
		orgList := OrgList{}
		proto.Unmarshal(b, &orgList)

		orgMap := make(map[string]*Organization)
		// convert to maps
		for _, v := range orgList.Org {
			orgMap[v.Org] = v
		}
		orgnMap = orgMap
	}
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
	f, err := os.Open("datafiles/OrgIds.csv")
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
