# fileP

Project to read large CSV parse them, convert them to protobuf and then save them to bolt db 
the csv file is the general datasets 
the first cut is reading entire file into memory and then processing it 

in terms of bolt, current the program just saves 1 million key value pairs to bolt. there have been a few learnings, e.g. saving 10k at a time is most efficient. 
Current work stopped at profiling of the memory and CPU utilization 

Commands to run 
 go run .\Orgbbolt.go 
 go run fileP.go .\orgMapping.go .\organization.pb.go 

 in trials 
 go run .\tryChan.go
 
 Profile using 
go tool pprof -http :8080 cpu.pprof    
go tool pprof -http :8080 mem.pprof     
go tool trace trace.out
