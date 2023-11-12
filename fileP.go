package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"time"
)

func main() {
	// defer profile.Start(profile.MemProfile, profile.MemProfileRate(1), profile.ProfilePath(".")).Stop()
	// defer profile.Start(profile.CPUProfile, profile.ProfilePath(".")).Stop()

	stT := time.Now()
	rec := readCsvFile("organizations-2000000.csv")
	// rec := readCsvFile("organizations-100.csv")
	endT := time.Since(stT)
	fmt.Println("time to parse ", len(rec), " records with columns ", len(rec[0]), " in  ", endT)
	//,  " size of [][]string ", size.Of(rec))

	stT = time.Now()
	// orgList, err := GetOrgsFromArrPlain(&rec)
	// orgList, err := GetOrgsFromArr(&rec)
	orgColl, err := GetOrgsFromArrSharded(&rec)

	st2 := time.Now()
	// orgMap, err := GetOrgsMapFromArr(&rec)
	// st3 := time.Now()
	// fmt.Println("time to map to list and maps are ", st2.Sub(stT), st3.Sub(stT))

	if err != nil {
		fmt.Println("error in parsing", err)
	}
	fmt.Println("Converting to sharded structure time taken ", (st2.Sub(stT)))

	stT = time.Now()
	err = orgColl.saveOrgColl("datafiles/op")
	st2 = time.Now()
	fmt.Println("Error was", err, "\nWritten all files ", len(orgColl), " time taken ", (st2.Sub(stT)))

	// Extract and save the validation countries
	// indCtryMap := extractIndCtry(orgList.Org)
	// fmt.Println(indCtryMap)
	// marshal the maps into JSON
	// b, err := json.Marshal(indCtryMap)
	// err = os.WriteFile("datafiles/valLists.json", b, 0777)

	// // output as JSon
	// stT = time.Now()
	// b, err = json.Marshal(&orgList)
	// endT = time.Since(stT)
	// fmt.Println("time to marshal as json - Error", err, " time taken ", endT) // , " size of bytes ", size.Of(b))
	// err = os.WriteFile("datafiles/org2m.json", b, 0777)

	// // output as protobuf
	// stT = time.Now()
	// b, err := proto.Marshal(&orgList)
	// if err != nil {
	// 	fmt.Println("error in protobuf marshalling", err)
	// }
	// endT = time.Since(stT)
	// fmt.Println(" after proto conversion, time take is ", endT, " and the size is ", len(b), " with verification", size.Of(b))

	// b2, err := proto.Marshal(&orgMap)
	// if err != nil {
	// 	fmt.Println("error in protobuf marshalling", err)
	// }

	// // write this to file.
	// stT = time.Now()
	// err = os.WriteFile("datafiles/org2mList.proto", b, 0777)
	// endT = time.Since(stT)
	// fmt.Println("Writing bytes file - Error", err, " time taken ", endT)
	// err = os.WriteFile("datafiles/org2mMap.proto", b2, 0777)
	// fmt.Println("Writing bytes 2 file - Error", err)

	// Write only the org ids back to the file as csv. we will do this by creating a [][]string and using encoder
	// orgIdsSlice := make([][]string, len(orgList.Org))
	// for i, v := range orgList.Org {
	// 	orgIdsSlice[i] = make([]string, 1)
	// 	orgIdsSlice[i][0] = v.Org
	// }
	// // fmt.Println(orgIdsSlice)
	// writeCsv(orgIdsSlice, "datafiles/OrgIds.csv")

}

func writeCsv(orgIdsSlice [][]string, fileName string) {
	f, err := os.Create(fileName)
	if err != nil {
		log.Fatal("Unable to write file ", err)
	}
	defer f.Close()
	csvWriter := *csv.NewWriter(f)
	csvWriter.WriteAll(orgIdsSlice)
	if err := csvWriter.Error(); err != nil {
		log.Fatalln("error writing csv:", err)
	}
}

func extractIndCtry(orgList []*Organization) map[string]map[string]int {
	indCtryMap := make(map[string]map[string]int)
	ctryMap := make(map[string]int)
	indMap := make(map[string]int)

	indCtryMap["Ctry"] = ctryMap
	indCtryMap["Ind"] = indMap

	for _, org := range orgList {
		ctry := org.Country
		ind := org.Industry

		addIncInMap(ctry, ctryMap)
		addIncInMap(ind, indMap)
	}
	return indCtryMap
}

func addIncInMap(key string, actMap map[string]int) {
	// actMap := *conMap
	_, present := actMap[key]
	if present {
		actMap[key]++
	} else {
		actMap[key] = 1
	}
}

func readCsvFile(filePath string) [][]string {
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

func GetOrgsFromArr(recArrPtr *[][]string) (OrgList, error) {

	recArr := *recArrPtr
	orgList := make([]*Organization, len(recArr)-1)

	for i := 1; i < len(recArr); i++ {
		orgArr := recArr[i]
		org, err := ParseOrgFromRec(&orgArr)
		if err != nil {
			return OrgList{}, err
		}
		// fmt.Println(org.ToString())
		orgList[i-1] = org
		//orgList = append(orgList, org)
	}
	ol := OrgList{Org: orgList}
	return ol, nil
}

func GetOrgsFromArrSharded(recArrPtr *[][]string) (*WriteShardedOrgColl, error) {

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

func GetOrgsMapFromArr(recArrPtr *[][]string) (OrgMap, error) {

	recArr := *recArrPtr
	orgsMap := make(map[string]*Organization, len(recArr)-1)

	for i := 1; i < len(recArr); i++ {
		orgArr := recArr[i]
		org, err := ParseOrgFromRec(&orgArr)
		if err != nil {
			return OrgMap{}, err
		}
		orgsMap[org.Org] = org
	}
	ol := OrgMap{OrgM: orgsMap}
	return ol, nil
}

func GetOrgsFromArrPlain(recArrPtr *[][]string) ([]*OrganizationPlain, error) {

	recArr := *recArrPtr
	orgList := make([]*OrganizationPlain, len(recArr)-1)

	for i := 1; i < len(recArr); i++ {
		orgArr := recArr[i]
		org, err := ParseOrgFromRecPlain(&orgArr)
		if err != nil {
			return orgList, err
		}
		orgList[i-1] = org
	}
	return orgList, nil
}

// Dont use this function as compresses in converting to gob
// use the function in the size package instead.

// func GetRealSizeOf(orgList []*OrganizationPlain) int {
// 	b := new(bytes.Buffer)
// 	if err := gob.NewEncoder(b).Encode(orgList); err != nil {
// 		fmt.Println("error in conv", err)
// 		return 0
// 	}
// 	return b.Len()
// }

// func printCsvFile(records [][]string) {
// 	for k, v := range records {
// 		fmt.Println(k, v)
// 		for _, val := range v {
// 			fmt.Print("|", val)
// 		}
// 		fmt.Println()
// 	}
// }
