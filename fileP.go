package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/ajayvm/fileP/size"
)

func main() {
	// defer profile.Start(profile.MemProfile, profile.MemProfileRate(1), profile.ProfilePath(".")).Stop()
	// defer profile.Start(profile.CPUProfile, profile.ProfilePath(".")).Stop()

	stT := time.Now()
	rec := readCsvFile("organizations-2000000.csv")
	// rec := readCsvFile("organizations-100.csv")
	endParse := time.Since(stT)
	fmt.Println("time to parse ", len(rec), " records with columns ", len(rec[0]), " in  ", endParse)

	stT = time.Now()
	orgList, err := GetOrgsFromArrPlain(&rec)
	// orgList, err := GetOrgsFromArr(&rec)
	endMapTime := time.Since(stT)
	fmt.Println("time to map ", endMapTime)
	if err != nil {
		fmt.Println("error in parsing", err)
	} else {
		// fmt.Println(len(orgList.Org), "; cap is ; ", cap(orgList.Org))
		fmt.Println(len(orgList), "; cap is ; ", cap(orgList), ": size ", size.Of(orgList))
	}

	// stT = time.Now()
	// protoBytes, err := proto.Marshal(&orgList)
	// if err != nil {
	// 	fmt.Println("error in protobuf marshalling", err)
	// }
	// protoTime := time.Since(stT)
	// fmt.Println(" after proto conversion, time take is ", protoTime.Microseconds(), " and the size is ", len(protoBytes))

	// populate into Database

	// populate into bbolt

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
