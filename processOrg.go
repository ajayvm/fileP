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
const jsonFile = "datafiles/org2m.json" // "datafiles/org100.json"
const jsonValFile = "datafiles/valLists.json"
const replaceBolt = true
const verifyInBolt = false

func main() {

	// 1. parse and get the country and org validations first
	bValJson, err := os.ReadFile(jsonValFile)
	if err != nil {
		panic(err)
	}
	ctryIndMap := make(map[string]map[string]int)
	json.Unmarshal(bValJson, &ctryIndMap)

	// fmt.Println("found country Ind Map ", ctryIndMap)

	tStart := time.Now()
	// 2. remove bolt db if write mode
	if replaceBolt {
		err := os.Remove("datafiles/orgbolt.db")
		if err != nil {
			fmt.Println("couldnt remove earlier bolt, ", err)
		}
	}
	tDel := time.Now()
	// fmt.Println("Time in deleting bolt", (st1.Sub(st0)))

	// 3. read main JSon file
	bJson, err := os.ReadFile(jsonFile)
	if err != nil {
		panic(err)
	}
	st2 := time.Now()
	// fmt.Println("Time in reading file", (st2.Sub(st1)))

	// 4. unmarshal into the object
	var orgList OrgList
	json.Unmarshal(bJson, &orgList)

	st3 := time.Now()
	// fmt.Println(" time in marshalling json ", (st3.Sub(st2)))

	// 5. Construct protobuf array
	orgMap := make(map[string][]byte)
	for _, org := range orgList.Org {
		// fmt.Println(org)
		b, err := proto.Marshal(org)
		if err != nil {
			fmt.Println("error in saving", err)
		}
		orgMap[org.Org] = b
	}
	st4 := time.Now()
	fmt.Println(" time in protobuf constr ", (st4.Sub(st3)))

	// test one unmarshal
	// orgT := Organization{}
	// proto.Unmarshal(orgMap["FAB0d41d5b5d22c"], &orgT)
	// fmt.Println("Unmarshalled Msg", orgT.Industry)

	// 6. Save protobuf to bolt db
	if replaceBolt {
		// save the country info, org info first
		saveCtryInds(ctryIndMap)
		// save the main data
		saveOrgs(orgMap)
	}
	st5 := time.Now()

	// fmt.Println(" time in saving org ", (st5.Sub(st4)))

	// 8. Bolt verification test
	if verifyInBolt {
		validateOrgs(orgList.Org)
	} else {
		verifyLocal(orgList.Org, ctryIndMap)
	}
	st6 := time.Now()
	// fmt.Println(" time in verification ", (st6.Sub(st5)))

	fmt.Println("Time in deleting existing bolt ", (tDel.Sub(tStart)), " reading file", (st2.Sub(tDel)),
		"\n time in marshalling json ", (st3.Sub(st2)), " time in protobuf constr ", (st4.Sub(st3)),
		"\n time in saving ", (st5.Sub(st4)), " time in verification", (st6.Sub(st5)),
		"\n Total time to parse, write, verify ", (st6.Sub(tDel)))

}

func verifyLocal(orgs []*Organization, ctryIndMap map[string]map[string]int) {

	// First verify country, then verify org ids
	ctryMap := ctryIndMap["Ctry"]
	indIdMap := ctryIndMap["Ind"]
	verifyCtryPresent(orgs, ctryMap)
	verifyIndPresent(orgs, indIdMap)
}

func verifyCtryPresent(orgs []*Organization, idMap map[string]int) {
	foundCtr := 0
	notFoundCtr := 0
	for _, org := range orgs {
		_, valueInDb := idMap[org.Country]
		if valueInDb {
			foundCtr++
		} else {
			notFoundCtr++
		}
	}
	fmt.Println("Found Country present stats F NF", foundCtr, notFoundCtr)
}

func verifyIndPresent(orgs []*Organization, idMap map[string]int) {
	foundCtr := 0
	notFoundCtr := 0
	for _, org := range orgs {
		_, valueInDb := idMap[org.Industry]
		if valueInDb {
			foundCtr++
		} else {
			notFoundCtr++
		}
	}
	fmt.Println("Found Country present stats F NF", foundCtr, notFoundCtr)
}
