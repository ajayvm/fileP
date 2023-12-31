# fileP

Project to read large CSV parse them, convert them to protobuf and then save them to bolt db 
the csv file is the general datasets from https://www.datablist.com/learn/csv/download-sample-csv-files 

For this POC, the organizations 100 and 2M record was used. https://github.com/datablist/sample-csv-files/raw/main/files/organizations/organizations-2000000.zip 

## Phase 1 (skip) - Trial runs 
* experimenting incremental / batched population in bolt db 
* go run .\Checkbbolt.go  

* Trying channels based on communications and wait groups and bolt db before implementation 
* go run .\tryChan.go

## Phase 2 - Preparatory run parsing public data set
main file is fileP.go. this has many sub function areas from reading CSV to saving validation lists and various json, protobufs. Uncomment various sections to check various functions 

* reading CSV, converting to proto / golang struct and then converting to proto / JSON and saving the json 
* go run fileP.go orgMapping.go organization.pb.go ShardedOrgStore.go

## Phase 3 - Main runs to parse JSon files to go struct, validate and save to Bolt and then load them all 

* for parsing json, converting to proto and populating bolt db run 
* go run processOrg.go orgMapping.go organization.pb.go Orgbbolt.go 

set the flags replace bolt and validate in bolt to true or false respective 

* For streaming based processing of the records 
* go run processOrgCh.go orgMapping.go organization.pb.go Orgbbolt.go

set the async processing flag to true if channel implementation required. 

* for only getting all values from the bolt given ids. 3 mechanisms are bolt, large slice, large map (1,2,3)
* go run useOrgs.go orgMapping.go organization.pb.go Orgbbolt.go  ShardedOrgStore.go 

Bolt based retrievals are very slow. Need investigation. even 2nd run is slow. 
Current work stopped at performance and total allocations. Memory and CPU profiling can be done next.  

## Phase 4 - Replace Bolt by sharded Protobuf structures 
* In this mechanism, the saving is done via lists that are sharded on key 
* Retrieval does a lazy load of shard based on the key and then constructs a map for perf reasons 

* Saving through fileP where directly a csv is converted to proto 
* go run fileP.go orgMapping.go organization.pb.go ShardedOrgStore.go 

* Saving from csv to object to protobuf on disk 
* go run processFromCSV.go orgMapping.go organization.pb.go ShardedOrgStore.go

* Saving from Json to object to sharded protobuf on disk
* go run saveShardOrgFJson.go ShardedOrgStore.go organization.pb.go

* Checking through the useOrgs for validity. Use mechanism 4 (sharded Map)
* go run useOrgs.go orgMapping.go organization.pb.go Orgbbolt.go   ShardedOrgStore.go

# Appendix 
 Profile using 
go tool pprof -http :8080 cpu.pprof    
go tool pprof -http :8080 mem.pprof     
go tool trace trace.out
