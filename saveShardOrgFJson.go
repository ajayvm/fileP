package main

import (
	"encoding/json"
	"fmt"
	"os"
	sync "sync"
	"time"
)

// read json using decoder into a record and then send it over channel
// 3 go routines, viz. first on file read, second on processing and third on db save

// constants
const orgJsonFileS = "datafiles/org2m.json" // "org100.json"
const jsonValidationFileS = "datafiles/valLists.json"
const isAsyncPipelineS = true

var wOColl WriteShardedOrgColl
var writeOrgColl = &wOColl

type valCtrs struct {
	valCtryCtr, inVCtryCtr, valIndCtr, inVIndCtr int
}

type processInfoS struct {
	org        *Organization
	ctryIndMap map[string]map[string]int
	validCtrs  *valCtrs
}

func main() {

	// 1. parse and get the country and org validations first
	stStart := time.Now()
	bValJson, err := os.ReadFile(jsonValidationFileS)
	if err != nil {
		panic(err)
	}
	ctryIndMap := make(map[string]map[string]int)
	json.Unmarshal(bValJson, &ctryIndMap)

	st1 := time.Now()

	// first read json as a single record
	input, err := os.Open(orgJsonFileS)
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

	recProcessed := 0
	valCtrsRun := valCtrs{}

	// Create channels if pipeline based processing is desired
	chProcess := make(chan processInfoS, 10000)
	var wg sync.WaitGroup

	if isAsyncPipelineS {
		chSave := make(chan *Organization, 10000)
		wg.Add(2)
		go asyncProcessSh(&wg, chProcess, chSave)
		go asyncSaveSh(&wg, chSave)
	}

	for dec.More() {
		var org Organization
		err := dec.Decode(&org)
		if err != nil {
			panic(err)
		}
		procInfo := processInfoS{org: &org, ctryIndMap: ctryIndMap, validCtrs: &valCtrsRun}

		if isAsyncPipelineS { // Push processInfo to a channel
			chProcess <- procInfo
		} else {
			// save information sequentially
			org, err := processSh(procInfo)
			if err != nil {
				fmt.Println("error in converting", err)
			}
			err = batchSaveToFiles(org)
			if err != nil {
				fmt.Println("error in saving", err)
			}
		}
		recProcessed++
	}
	close(chProcess)
	if isAsyncPipelineS {
		fmt.Println("Waiting for processes to complete.")
		wg.Wait()
	}
	writeOrgColl.saveOrgColl("datafiles/op")
	stEnd := time.Now()
	fmt.Println("is async : ", isAsyncPipelineS, recProcessed, " records processed in ", (stEnd.Sub(st1)),
		" total time ", (stEnd.Sub(stStart)), " Valid country ", valCtrsRun.valCtryCtr, valCtrsRun.inVCtryCtr,
		" Valid Ind ", valCtrsRun.valIndCtr, valCtrsRun.inVIndCtr)
}

func asyncSaveSh(wg *sync.WaitGroup, chSave chan *Organization) {
	defer wg.Done()
	for org := range chSave {
		err := batchSaveToFiles(org)
		if err != nil {
			fmt.Println("error in saving", err)
		}
	}
}

func asyncProcessSh(wg *sync.WaitGroup, chProcess chan processInfoS, chSave chan *Organization) {
	defer wg.Done()
	for procInfo := range chProcess {
		savInfo, err := processSh(procInfo)
		if err != nil {
			fmt.Println("error in converting", err)
		}
		chSave <- savInfo
	}
	close(chSave)
}

// validate data, convert to protobuf and return byte array
func processSh(procInfo processInfoS) (*Organization, error) {

	org := procInfo.org
	valCtrs := procInfo.validCtrs

	// Do validations for ctry and Industry
	_, isValInMap := procInfo.ctryIndMap["Ctry"][org.Country]
	if isValInMap {
		valCtrs.valCtryCtr++
	} else {
		valCtrs.inVCtryCtr++
	}
	_, isValInMap = procInfo.ctryIndMap["Ind"][org.Industry]
	if isValInMap {
		valCtrs.valIndCtr++
	} else {
		valCtrs.inVIndCtr++
	}

	return org, nil
}

func batchSaveToFiles(org *Organization) error {
	writeOrgColl = writeOrgColl.addOrg(org)
	return nil
}
