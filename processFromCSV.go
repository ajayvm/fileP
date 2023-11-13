package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"time"

	"google.golang.org/protobuf/proto"
)

const useSharding = true

func main() {
	// defer profile.Start(profile.MemProfile, profile.MemProfileRate(1), profile.ProfilePath(".")).Stop()
	// defer profile.Start(profile.CPUProfile, profile.ProfilePath(".")).Stop()

	stT := time.Now()
	rec := readFullCsvFile("organizations-2000000.csv")
	// rec := readCsvFile("organizations-100.csv")
	endT := time.Since(stT)
	fmt.Println("1. time to parse ", len(rec), " records with columns ", len(rec[0]), " in  ", endT)
	//,  " size of [][]string ", size.Of(rec))

	if useSharding {
		stT = time.Now()
		orgColl, err := GetAllOrgsFromArrSharded(&rec)
		st2 := time.Now()
		fmt.Println("2. Converting to sharded structure time taken ", (st2.Sub(stT)))
		// saving the sharded Maps
		stT = time.Now()
		err = orgColl.saveOrgColl("datafiles/op")
		st2 = time.Now()
		fmt.Println("3. Written all sharded files ", len(orgColl), " time taken ", (st2.Sub(stT)), " error ", err)
	} else {
		stT = time.Now()
		orgList, err := GetAllOrgsFromArr(&rec)
		st3 := time.Now()
		fmt.Println("3. time to map to list is ", st3.Sub(stT))

		// output as protobuf
		stT = time.Now()
		b, err := proto.Marshal(orgList)
		if err != nil {
			fmt.Println("error in protobuf marshalling", err)
		}
		endT = time.Since(stT)
		fmt.Println("7. after proto conversion to list, time take is ", endT, " and the size is ", len(b))
		// , " with verification", size.Of(b))
		stT = time.Now()
		err = os.WriteFile("datafiles/org2mList.proto", b, 0777)
		endT = time.Since(stT)
		fmt.Println("8. Writing Org list proto file - Error", err, " time taken ", endT)
	}
}

func readFullCsvFile(filePath string) [][]string {
	f, err := os.Open(filePath)
	if err != nil {
		log.Fatal("Unable to read input file "+filePath, err)
	}
	defer f.Close()

	csvReader := csv.NewReader(f)
	records, err := csvReader.ReadAll()
	if err != nil {
		log.Fatal("Unable to parse file as CSV for "+filePath, err)
	}
	return records
}

func GetAllOrgsFromArr(recArrPtr *[][]string) (*OrgList, error) {

	recArr := *recArrPtr
	orgList := make([]*Organization, len(recArr)-1)

	for i := 1; i < len(recArr); i++ {
		orgArr := recArr[i]
		org, err := ParseOrgFromRec(&orgArr)
		if err != nil {
			return &OrgList{}, err
		}
		// fmt.Println(org.ToString())
		orgList[i-1] = org
		//orgList = append(orgList, org)
	}
	ol := OrgList{Org: orgList}
	return &ol, nil
}

func GetAllOrgsFromArrSharded(recArrPtr *[][]string) (*WriteShardedOrgColl, error) {

	recArr := *recArrPtr
	var oCol WriteShardedOrgColl
	orgColl := &oCol

	for i := 1; i < len(recArr); i++ {
		orgArr := recArr[i]
		org, err := ParseOrgFromRec(&orgArr)
		if err != nil {
			return orgColl, err
		}
		orgColl = orgColl.addOrg(org)
	}
	return orgColl, nil
}
