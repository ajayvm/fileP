package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/ajayvm/fileP/size"
	bolt "go.etcd.io/bbolt"
)

var bPath = "orgbb.db"

func main() {
	// defer profile.Start(profile.CPUProfile, profile.ProfilePath(".")).Stop()
	// defer profile.Start(profile.MemProfile, profile.ProfilePath(".")).Stop() // profile.MemProfileRate(1)

	kvMap := make(map[string]string)

	for i := 0; i < 100000; i++ {
		iStr := strconv.Itoa(i)
		kvMap["k"+iStr] = "v" + iStr
	}
	fmt.Println("size of main map ", size.Of(kvMap))

	db, err := bolt.Open(bPath, 0666, nil)
	if err != nil {
		fmt.Println("error init db", err)
		return
	}
	defer db.Close()

	ts1 := time.Now()
	ts11 := ts1
	kvCtr := 0
	kvChildMap := make(map[string]string)
	for k, v := range kvMap {
		kvCtr++
		kvChildMap[k] = v
		if kvCtr%10000 == 0 {
			ts2 := time.Now()
			saveToDb(db, kvChildMap)
			fmt.Println(kvCtr, " took ", ts2.Sub(ts11))
			ts11 = ts2
			kvChildMap = make(map[string]string)
		}
	}
	// Now save remaining entries if present
	if len(kvChildMap) > 0 {
		saveToDb(db, kvChildMap)
		fmt.Println("save remaining ", len(kvChildMap), " entries ")
	}

	ts2 := time.Now()
	readFromDb(db)
	ts3 := time.Now()
	val, _ := readFromDb(db)
	ts4 := time.Since(ts3)
	fmt.Println("time taken file opening ", ts2.Sub(ts1), " time reading a key ", ts4, "value is ", val)
}

func saveToDb(db *bolt.DB, kvMap map[string]string) error {
	err := db.Update(func(tx *bolt.Tx) error {
		orgBucket, err := tx.CreateBucketIfNotExists([]byte("OrgBucket"))
		if err != nil {
			return fmt.Errorf("could not create root bucket: %v", err)
		}
		//ctr := 0
		// ts11 := time.Now()
		for k, v := range kvMap {
			orgBucket.Put([]byte(k), []byte(v))
			// ctr++
			// if ctr%1000 == 0 {
			// 	ts22 := time.Now()
			// 	// fmt.Println(ctr, " took ", ts22.Sub(ts11))
			// 	ts11 = ts22
			// }
		}
		return nil
	})
	return err
}

func readFromDb(db *bolt.DB) (string, error) {
	valueInDb := ""
	err := db.View(func(tx *bolt.Tx) error {
		orgBucket := tx.Bucket([]byte("OrgBucket"))
		// v := string(orgBucket.Get([]byte("Key")))
		valueInDb = string(orgBucket.Get([]byte("k0")))
		// fmt.Println("value is ", v, v2, " other values ", orgBucket.Stats(), " orgB ")
		// json.NewEncoder(os.Stderr).Encode(orgBucket.Stats())
		return nil
	})
	return valueInDb, err
}
