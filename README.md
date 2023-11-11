# fileP

Project to read large CSV parse them, convert them to protobuf and then save them to bolt db 
the csv file is the general datasets from https://www.datablist.com/learn/csv/download-sample-csv-files


in terms of bolt, current the program just saves 1 million key value pairs to bolt. there have been a few learnings, e.g. saving 10k at a time is most efficient. Refer Learnings.docx for more details and benchmarks for numbers 

Commands to run 
experimenting incremental / batched population in bolt db 
 go run .\Orgbbolt.go 

Preparatory runs 
* reading CSV, converting to proto / golang struct and then converting to proto / JSON and saving the json 
 go run fileP.go .\orgMapping.go .\organization.pb.go 

* Trying channels based on communications and wait groups and bolt db before implementation 
go run .\tryChan.go

go run .\Checkbbolt.go  

Main Runs 

* for parsing json, converting to proto and populating bolt db run 
go run processOrg.go orgMapping.go organization.pb.go Orgbbolt.go 

set the flags replace bolt and validate in bolt to true or false respective 

* For streaming based processing of the records 
go run processOrgCh.go orgMapping.go organization.pb.go Orgbbolt.go

set the async processing flag to true if channel implementation required. 

* for only getting all values from the bolt given ids 
go run useOrgs.go orgMapping.go organization.pb.go Orgbbolt.go   

Current work stopped at profiling of the memory and CPU utilization 


 Profile using 
go tool pprof -http :8080 cpu.pprof    
go tool pprof -http :8080 mem.pprof     
go tool trace trace.out
