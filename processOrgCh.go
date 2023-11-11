package main

import (
	"encoding/json"
	"fmt"
	"os"
	sync "sync"
	"time"

	bolt "go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"
)

// read json using decoder into a record and then send it over channel
// 3 go routines, viz. first on file read, second on processing and third on db save

// constants
const orgJsonFile = "datafiles/org2m.json" // "org100.json"
const jsonValidationFile = "datafiles/valLists.json"
const isAsyncPipeline = true

type processInfo struct {
	org                   *Organization
	ctryIndMap            map[string]map[string]int
	valCtryMap, valIndMap map[string]int
}

type saveInfo struct {
	key string
	val []byte
}

func main() {

	// 1. parse and get the country and org validations first
	stStart := time.Now()
	bValJson, err := os.ReadFile(jsonValidationFile)
	if err != nil {
		panic(err)
	}
	ctryIndMap := make(map[string]map[string]int)
	json.Unmarshal(bValJson, &ctryIndMap)

	// 1. remove the existing bolt db if required
	err = os.Remove("datafiles/orgbolt.db")
	if err != nil {
		fmt.Println("couldnt remove earlier bolt, ", err)
	}
	saveCtryInds(ctryIndMap)
	st1 := time.Now()

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
	validCtryMap := make(map[string]int)
	validIndMap := make(map[string]int)
	validCtryMap["Found"] = 0
	validCtryMap["NotFound"] = 0
	validIndMap["Found"] = 0
	validIndMap["NotFound"] = 0

	// Create channels if pipeline based processing is desired
	chProcess := make(chan processInfo, 10000)
	var wg sync.WaitGroup

	if isAsyncPipeline {
		chSave := make(chan saveInfo, 10000)
		wg.Add(2)
		go asyncProcess(&wg, chProcess, chSave)
		go asyncSave(&wg, chSave)
	}

	for dec.More() {
		var org Organization
		err := dec.Decode(&org)
		if err != nil {
			panic(err)
		}
		procInfo := processInfo{org: &org, ctryIndMap: ctryIndMap, valCtryMap: validCtryMap,
			valIndMap: validIndMap}

		if isAsyncPipeline { // Push processInfo to a channel
			chProcess <- procInfo
		} else {
			// save information sequentially
			savInfo, err := process(procInfo)
			if err != nil {
				fmt.Println("error in converting", err)
			}
			err = batchSaveToDb(savInfo)
			if err != nil {
				fmt.Println("error in saving", err)
			}
		}
		recProcessed++
	}
	close(chProcess)
	if isAsyncPipeline {
		fmt.Println("Waiting for processes to complete.")
		wg.Wait()
	}
	closeBoltDb()
	stEnd := time.Now()
	fmt.Println("is async : ", isAsyncPipeline, recProcessed, " records processed in ", (stEnd.Sub(st1)),
		" total time ", (stEnd.Sub(stStart)), " Valid country ", validCtryMap, " Valid Ind ", validIndMap)

	checkFirstId()
}

func asyncSave(wg *sync.WaitGroup, chSave chan saveInfo) {
	defer wg.Done()
	for savInfo := range chSave {
		err := batchSaveToDb(savInfo)
		if err != nil {
			fmt.Println("error in saving", err)
		}
	}
}

func asyncProcess(wg *sync.WaitGroup, chProcess chan processInfo, chSave chan saveInfo) {
	defer wg.Done()
	for procInfo := range chProcess {
		savInfo, err := process(procInfo)
		if err != nil {
			fmt.Println("error in converting", err)
		}
		chSave <- savInfo
	}
	close(chSave)
}

// validate data, convert to protobuf and return byte array
func process(procInfo processInfo) (saveInfo, error) {

	org := procInfo.org
	validCtryMap := procInfo.valCtryMap
	validIndMap := procInfo.valIndMap

	orgId := org.Org
	b, err := proto.Marshal(org)

	// Do validations for ctry and Industry
	_, isValInMap := procInfo.ctryIndMap["Ctry"][org.Country]
	if isValInMap {
		validCtryMap["Found"]++
	} else {
		validCtryMap["NotFound"]++
	}
	_, isValInMap = procInfo.ctryIndMap["Ind"][org.Industry]
	if isValInMap {
		validIndMap["Found"]++
	} else {
		validIndMap["NotFound"]++
	}

	return saveInfo{orgId, b}, err
}

// Heap level variables that need to be maintained because of record by record processing
// TBD better after PoC
var bdb *bolt.DB
var orgMap map[string][]byte
var batchCtr int

func batchSaveToDb(savInfo saveInfo) error {

	// below use variables from the OrgBbolt.go
	orgMap[savInfo.key] = savInfo.val
	batchCtr++
	if batchCtr%10000 == 0 {
		err := saveByteMapsToDb(bdb, orgMap, OrgBucketName)
		if err != nil {
			return err
		}
		orgMap = make(map[string][]byte)
		batchCtr = 0
	}
	return nil
}

func initBoltDb() {

	if bdb == nil {
		var err bool
		bdb, err = openBolt()
		if err {
			panic("Error opening db")
		}
	}
	orgMap = make(map[string][]byte)
	batchCtr = 0
}

// Ensure that the map is fully copied and close connection
func closeBoltDb() {

	if bdb != nil && orgMap != nil && len(orgMap) > 0 {
		err := saveByteMapsToDb(bdb, orgMap, OrgBucketName)
		if err != nil {
			panic(err)
		}
	}
	if bdb != nil {
		bdb.Close()
	}
}
