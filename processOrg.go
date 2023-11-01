package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"google.golang.org/protobuf/proto"
)

// Shows 3 paths to parse JSON, then do validations and then save to Bolt
// 1. Parse all JSON, convert to proto, then save to bolt, in batches of 1000
// 2. Parse all JSON, convert to proto, validate first before saving to bolt
// 3. (separate golang file) parse each JSON --> Stream process of validation, proto conversion and saving to bolt

// constants
const jsonFile = "org100.json" // "org2m.json" // "org100.json"
const replaceBolt = false
const verifyInBolt = false

func main() {

	tStart := time.Now()
	if replaceBolt {
		err := os.Remove("orgbolt.db")
		if err != nil {
			panic(err)
		}
	}
	tDel := time.Now()
	// fmt.Println("Time in deleting bolt", (st1.Sub(st0)))

	bJson, err := os.ReadFile(jsonFile)
	if err != nil {
		panic(err)
	}
	st2 := time.Now()
	// fmt.Println("Time in reading file", (st2.Sub(st1)))

	var orgList OrgList
	json.Unmarshal(bJson, &orgList)

	st3 := time.Now()
	// fmt.Println(" time in marshalling json ", (st3.Sub(st2)))

	orgMap := make(map[string][]byte)
	for _, org := range orgList.Org {
		fmt.Println(org)
		b, err := proto.Marshal(org)
		if err != nil {
			fmt.Println("error in saving", err)
		}
		orgMap[org.Org] = b
	}
	st4 := time.Now()
	// fmt.Println(" time in protobuf constr ", (st4.Sub(st3)))
	orgIdMap := make(map[string]struct{})
	for _, org := range orgList.Org {
		orgIdMap[org.Org] = struct{}{}
	}
	if replaceBolt {
		saveOrgs(orgMap)
	}
	st5 := time.Now()
	// fmt.Println(" time in saving org ", (st5.Sub(st4)))
	if verifyInBolt {
		verifyOrgs(orgList.Org)
	} else {
		verifyLocal(orgList.Org, orgIdMap)
	}
	st6 := time.Now()
	// fmt.Println(" time in verification ", (st6.Sub(st5)))

	fmt.Println("Time in deleting existing bolt ", (tDel.Sub(tStart)), " reading file", (st2.Sub(tDel)),
		"\n time in marshalling json ", (st3.Sub(st2)), " time in protobuf constr ", (st4.Sub(st3)),
		"\n time in saving ", (st5.Sub(st4)), " time in verification", (st6.Sub(st5)),
		"\n Total end to end time ", (st6.Sub(tDel)))
}

func verifyLocal(orgs []*Organization, orgIdMap map[string]struct{}) {
	orgF := make(map[string]int)
	orgF["Found"] = 0
	orgF["NotFound"] = 0
	for _, org := range orgs {
		_, valueInDb := orgIdMap[org.Org]
		if valueInDb {
			orgF["Found"]++
		} else {
			orgF["NotFound"]++
		}
	}
	fmt.Println("Found stats", orgF)
}
