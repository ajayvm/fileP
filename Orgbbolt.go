package main

// This class has functions related to saving and checking from Boltdb
// it abstracts internals of Bolt db with external world and exposes ORM kind of functions

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	bolt "go.etcd.io/bbolt"
)

const OrgBoltPath = "datafiles/orgbolt.db"
const OrgBucketName = "OrgBucket"
const IndIdBucketName = "IndIdBucket"
const CtryBucketName = "CtryBucket"

// this function saves the entire map of organizations in batches of 10000
// bolt db is most efficient in saving 10k objects at a time
// this is most heavy performing methods. to check if paralelization would help
func saveOrgs(orgsMap map[string][]byte) {

	db, shouldReturn := openBolt()
	if shouldReturn {
		return
	}
	defer db.Close()

	// save the org in batches of 10,000
	orgMap := make(map[string][]byte)
	ctr := 0
	for k, v := range orgsMap {
		orgMap[k] = v
		ctr++
		if ctr%10000 == 0 {
			saveByteMapsToDb(db, orgMap, OrgBucketName)
			orgMap = make(map[string][]byte)
			ctr = 0
		}
	}
	// Now save remaining entries if present
	if len(orgMap) > 0 {
		saveByteMapsToDb(db, orgMap, OrgBucketName)
		fmt.Println("save remaining ", len(orgMap), " entries ")
	}
}

func saveCtryInds(ctryIndMap map[string]map[string]int) {

	// first save countries, and then similarly save industries
	ctryMap := ctryIndMap["Ctry"]
	indIdMap := ctryIndMap["Ind"]
	ctryBytesMap := convIntToBytes(ctryMap)
	indIdBytesMap := convIntToBytes(indIdMap)

	db, shouldReturn := openBolt()
	if shouldReturn {
		return
	}
	defer db.Close()

	saveByteMapsToDb(db, ctryBytesMap, CtryBucketName)
	saveByteMapsToDb(db, indIdBytesMap, IndIdBucketName)
}

func convIntToBytes(intMap map[string]int) map[string][]byte {
	byteMap := make(map[string][]byte)
	for k, v := range intMap {
		byteMap[k] = []byte(strconv.Itoa(v))
	}
	return byteMap
}

func validateOrgs(orgs []*Organization) {
	db, shouldReturn := openBolt()
	if shouldReturn {
		return
	}
	defer db.Close()

	orgF, err := checkCtryinBolt(db, orgs)
	fmt.Println("Found stats for country ", orgF, ":err ", err)
	orgF, err = checkIndinBolt(db, orgs)
	fmt.Println("Found stats for Industry ", orgF, ":err ", err)
}

func getAllObj(db *bolt.DB, orgs []string) map[string][]byte {
	foundCtr := 0
	notFoundCtr := 0

	orgBMap := make(map[string][]byte)

	err := db.View(func(tx *bolt.Tx) error {
		orgBucket := tx.Bucket([]byte(OrgBucketName))
		for _, org := range orgs {
			valueInDb := orgBucket.Get([]byte(org))
			if valueInDb == nil {
				notFoundCtr++
			} else {
				foundCtr++
			}
			// Remember to copy the bytes if the result is to be used outside
			bArrCpy := make([]byte, len(valueInDb))
			copy(bArrCpy, valueInDb)
			orgBMap[org] = bArrCpy
		}
		// json.NewEncoder(os.Stderr).Encode(orgBucket.Stats())
		return nil
	})
	fmt.Println("Found stats for Org Found Not Found", foundCtr, notFoundCtr, ":err ", err)
	return orgBMap
}

func checkFirstId() {
	db, shouldReturn := openBolt()
	if shouldReturn {
		return
	}
	defer db.Close()

	checkFirstIdInBolt(db)
}

func checkFirstIdInBolt(db *bolt.DB) {
	err := db.View(func(tx *bolt.Tx) error {
		orgBucket := tx.Bucket([]byte(OrgBucketName))
		// fmt.Println(orgBucket.Cursor().First())
		json.NewEncoder(os.Stderr).Encode(orgBucket.Stats())
		return nil
	})
	if err != nil {
		fmt.Println(err)
	}
}

func checkCtryinBolt(db *bolt.DB, orgs []*Organization) (map[string]int, error) {

	orgF := make(map[string]int)
	orgF["Found"] = 0
	orgF["NotFound"] = 0
	err := db.View(func(tx *bolt.Tx) error {
		orgBucket := tx.Bucket([]byte(CtryBucketName))
		for _, org := range orgs {
			valueInDb := orgBucket.Get([]byte(org.Country))
			if valueInDb == nil {
				orgF["NotFound"]++
			} else {
				orgF["Found"]++
			}
		}
		// json.NewEncoder(os.Stderr).Encode(orgBucket.Stats())
		return nil
	})
	return orgF, err
}

func checkIndinBolt(db *bolt.DB, orgs []*Organization) (map[string]int, error) {

	orgF := make(map[string]int)
	orgF["Found"] = 0
	orgF["NotFound"] = 0
	err := db.View(func(tx *bolt.Tx) error {
		orgBucket := tx.Bucket([]byte(IndIdBucketName))
		for _, org := range orgs {
			valueInDb := orgBucket.Get([]byte(org.Industry))
			if valueInDb == nil {
				orgF["NotFound"]++
			} else {
				orgF["Found"]++
			}
		}
		// json.NewEncoder(os.Stderr).Encode(orgBucket.Stats())
		return nil
	})
	return orgF, err
}

func openBolt() (*bolt.DB, bool) {
	db, err := bolt.Open(OrgBoltPath, 0666, nil)
	if err != nil {
		fmt.Println("error init db", err)
		return nil, true
	}
	return db, false
}

func saveByteMapsToDb(db *bolt.DB, kvMap map[string][]byte, bucketName string) error {
	err := db.Update(func(tx *bolt.Tx) error {
		orgBucket, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		if err != nil {
			return fmt.Errorf("could not create bucket: %v", err)
		}
		for k, v := range kvMap {
			orgBucket.Put([]byte(k), v)
			// fmt.Println("inserting key ", k)
		}
		return nil
	})
	return err
}
