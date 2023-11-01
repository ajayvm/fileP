package main

import (
	"fmt"

	bolt "go.etcd.io/bbolt"
)

const OrgBoltPath = "orgbolt.db"

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
			saveOrgToDb(db, orgMap)
			orgMap = make(map[string][]byte)
			ctr = 0
		}
	}
	// Now save remaining entries if present
	if len(orgMap) > 0 {
		saveOrgToDb(db, orgMap)
		fmt.Println("save remaining ", len(orgMap), " entries ")
	}
}

func verifyOrgs(orgs []*Organization) {
	db, shouldReturn := openBolt()
	if shouldReturn {
		return
	}
	defer db.Close()

	orgF, err := checkinBolt(db, orgs)
	fmt.Println("Found stats", orgF, ":err ", err)
}

func checkinBolt(db *bolt.DB, orgs []*Organization) (map[string]int, error) {

	orgF := make(map[string]int)
	orgF["Found"] = 0
	orgF["NotFound"] = 0
	err := db.View(func(tx *bolt.Tx) error {
		orgBucket := tx.Bucket([]byte("OrgBucket"))
		for _, org := range orgs {
			valueInDb := orgBucket.Get([]byte(org.Org))
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

func saveOrgToDb(db *bolt.DB, kvMap map[string][]byte) error {
	err := db.Update(func(tx *bolt.Tx) error {
		orgBucket, err := tx.CreateBucketIfNotExists([]byte("OrgBucket"))
		if err != nil {
			return fmt.Errorf("could not create bucket: %v", err)
		}
		for k, v := range kvMap {
			orgBucket.Put([]byte(k), v)
		}
		return nil
	})
	return err
}

var bdb *bolt.DB
var orgMap map[string][]byte
var batchCtr int

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
		err := saveOrgToDb(bdb, orgMap)
		if err != nil {
			panic(err)
		}
	}
	if bdb != nil {
		bdb.Close()
	}
}

func batchSaveToDb(key string, b []byte) error {

	orgMap[key] = b
	batchCtr++
	if batchCtr%10000 == 0 {
		err := saveOrgToDb(bdb, orgMap)
		if err != nil {
			return err
		}
		orgMap = make(map[string][]byte)
		batchCtr = 0
	}
	return nil
}
