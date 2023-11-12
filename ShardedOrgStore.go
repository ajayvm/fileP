package main

import (
	"os"
	"strconv"
	sync "sync"

	"github.com/segmentio/fasthash/fnv1a"
	"google.golang.org/protobuf/proto"
)

const NUM_SHARDS = 32 // Randomly picked from 2^5. can be tuned later.

// concept inspired from https://github.com/zutto/shardedmap/blob/master/ShardedMap.go
// however, use case we have is separation of read and write use cases where
// 1. Writes happen at one shot via lists to proto files
// 2. an efficient read only sharded map that is initialized from file store

type ShardOrgMap struct {
	Lock   sync.RWMutex
	orgMap map[string]*Organization
}

// Our Read Structure is a Sharded Map for fast reads
type ReadShardedOrgMap [NUM_SHARDS]*ShardOrgMap

// Write Structure is a Sharded Collection as for faster marshal and unmarshall from Proto
// typical use cases have read or write, never both based for Read only cases.
type WriteShardedOrgColl [NUM_SHARDS][]*Organization

// Now various methods to add to the Write Collection & save it and get from the Read Collection

// this method figures the shard and then adds to the write collection.
func (orgColl *WriteShardedOrgColl) addOrg(org *Organization) *WriteShardedOrgColl {

	// Get key and find the shard
	shardNo := GetShardNoForStr(org.Org, NUM_SHARDS)
	orgList := orgColl[shardNo]
	if orgList == nil {
		orgList = make([]*Organization, 0, 100)
	}
	orgList = append(orgList, org)
	orgColl[shardNo] = orgList
	// fmt.Println("Collection Size for Shard ", shardNo, " is ", len(orgList))
	return orgColl
}

// save the collection as number of sharded files
func (orgColl *WriteShardedOrgColl) saveOrgColl(location string) error {

	// clean up directory, then add new files
	err := os.RemoveAll(location)
	if err != nil {
		return err
	}
	err = os.MkdirAll(location, os.ModePerm)
	if err != nil {
		return err
	}

	for i, shardOfOrgs := range orgColl {
		orgList := OrgList{Org: shardOfOrgs}
		b, err := proto.Marshal(&orgList)
		if err != nil {
			return err
		}
		err = os.WriteFile(location+"/Org"+strconv.Itoa(i)+".pro", b, 0777)
		if err != nil {
			return err
		}
	}
	return nil
}

// get a single value
func (orgFullMap *ReadShardedOrgMap) getOrg(key string) (*Organization, bool, error) {
	// Get key and find the shard
	shardNo := GetShardNoForStr(key, NUM_SHARDS)
	orgSMap := orgFullMap[shardNo]
	orgMap := orgSMap.orgMap

	// return values if map is populated
	if len(orgMap) > 0 {
		org, found := orgSMap.orgMap[key]
		// fmt.Println("Found in first check for shard ", shardNo, " key ", key)
		return org, found, nil
	}
	// since map is empty, now fill information from file system
	orgSMap.Lock.Lock()
	defer orgSMap.Lock.Unlock()

	// Recheck if the call that escaped first check from another go routine is now caught in second check
	orgSMap = orgFullMap[shardNo]
	orgMap = orgSMap.orgMap
	if len(orgMap) > 0 {
		// fmt.Println("Found in second check for shard ", shardNo, " key ", key)
		org, found := orgSMap.orgMap[key]
		return org, found, nil
	}
	// now start the population from file --> bytes --> proto --> list --> Map
	fileName := "datafiles/op/Org" + strconv.Itoa(int(shardNo)) + ".pro"
	b, err := os.ReadFile(fileName)
	if err != nil {
		return nil, false, err
	}
	var orgList OrgList
	proto.Unmarshal(b, &orgList)

	// convert to maps. note that in proto always list is faster, hence this one time conversion
	for _, v := range orgList.Org {
		orgMap[v.Org] = v
	}
	orgSMap.orgMap = orgMap // reassign populated map back to the main shardedMap structure
	org, found := orgMap[key]
	// fmt.Println("Found finally shard ", shardNo, " key ", key)
	return org, found, nil
}

// initialize the sharded Map structure.
// only for the read use case since it will help initialize the locks
// For write, the var initialization is enough. see fileP.go
func (orgFullMap *ReadShardedOrgMap) InitSMap() *ReadShardedOrgMap {

	for i := 0; i < NUM_SHARDS; i++ {
		orgFullMap[i] = &ShardOrgMap{orgMap: make(map[string]*Organization)}
	}
	return orgFullMap
}

func GetShardNoForStr(inp string, shards uint64) uint64 {
	h := fnv1a.HashString64(inp)
	return h % shards
}
