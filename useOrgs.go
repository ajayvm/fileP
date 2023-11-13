package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"runtime"
	"time"

	bolt "go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"
)

// this class is to get all organizations and unmarshal them for proto
// it will help to assess the memory and processing overheads of proto and the bolt serialization

const mechGet = 3 // 1 - bolt , 2 - from big map, 3 - from big list, 4 - from sharded list

// shift below to the inside methods
var orgnMap = make(map[string]*Organization)

func main() {
	orgIds := getOrgsFromCsv()
	// we are determining memory allocations at 3 times, viz. Start, first pass and end of pass
	var startMemory runtime.MemStats
	var midMemory runtime.MemStats
	var endMemory runtime.MemStats

	runtime.ReadMemStats(&startMemory)

	// Now unmarshal all two times to see effect of caching
	if mechGet == 1 { // from Bolt

		db, _ := openBolt()
		defer db.Close()

		getFromBolt(db, orgIds)
		runtime.ReadMemStats(&midMemory)
		getFromBolt(db, orgIds)
	} else if mechGet == 2 || mechGet == 3 { // full map or List
		getFromProto(orgIds)
		runtime.ReadMemStats(&midMemory)
		getFromProto(orgIds)
	} else if mechGet == 4 { // sharded list

		var oMap ReadShardedOrgMap
		orgShardedMap := &oMap
		orgShardedMap = orgShardedMap.InitSMap()
		getFromShardedProto(orgShardedMap, orgIds)
		runtime.ReadMemStats(&midMemory)
		getFromShardedProto(orgShardedMap, orgIds)
	}
	runtime.ReadMemStats(&endMemory)

	fmt.Println("Total allocations after first pass and 2nd pass:",
		(midMemory.TotalAlloc - startMemory.TotalAlloc), (endMemory.TotalAlloc - midMemory.TotalAlloc))
}

func getFromShardedProto(orgShardedMap *ReadShardedOrgMap, orgIds []string) {
	st6 := time.Now()
	// now search for all passed ids and return orgs that match
	foundCtr := 0
	notFoundCtr := 0

	for _, v := range orgIds {
		_, found, _ := orgShardedMap.getOrg(v)
		if found {
			foundCtr++
		} else {
			notFoundCtr++
		}
	}
	st7 := time.Now()

	fmt.Println("Mechanism:", mechGet, "Time to search ", (st7.Sub(st6)),
		" report F NF", foundCtr, notFoundCtr,
		" for records  ", len(orgIds))
}

func getFromProto(orgIds []string) {
	st6 := time.Now()
	if mechGet == 2 {
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

	fmt.Println("Mechanism:", mechGet, "Time to unmarshal ", (st7.Sub(st6)),
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

func getFromBolt(db *bolt.DB, orgIds []string) {

	st6 := time.Now()
	bytesInDb := getAllObj(db, orgIds)
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
