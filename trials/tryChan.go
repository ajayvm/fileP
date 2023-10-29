package main

import (
	"fmt"
	"sync"
	"time"
)

func main() {
	// defer profile.Start(profile.CPUProfile, profile.ProfilePath(".")).Stop()
	// defer profile.Start(profile.GoroutineProfile, profile.ProfilePath(".")).Stop()
	// defer profile.Start(profile.TraceProfile, profile.ProfilePath(".")).Stop()

	ctr := 100

	st1 := time.Now()
	state := syncExecS(ctr)
	endT := time.Since(st1)
	fmt.Println("Ch closed. Sum is ", state, " and time taken is ", endT)

	st1 = time.Now()
	state = asyncExec(ctr)
	endT = time.Since(st1)
	fmt.Println("Ch closed. Sum is ", state, " and time taken is ", endT)
}

func syncExecS(ctr int) int {

	state := 0
	for i := 0; i < ctr; i++ {
		st1 := mult2(i)
		st1 = mult2(st1)
		state += st1
	}
	return state
}

func asyncExec(ctr int) int {
	c1 := make(chan int)
	c2 := make(chan int)
	c3 := make(chan int)

	var wg sync.WaitGroup
	wg.Add(3)

	go process("s2", &wg, c1, c2)
	go process("s3", &wg, c2, c3)

	state := 0
	go finish(c3, &wg, &state)
	fmt.Println("Ch still open. Sum is ", state)

	for i := 0; i < ctr; i++ {
		c1 <- i
	}
	close(c1)
	wg.Wait()
	return state
}

func finish(c3 chan int, wg *sync.WaitGroup, state *int) {
	defer wg.Done()
	for i := range c3 {
		// fmt.Println("Stage finalize. Got ", i)
		*state += i
	}
}

func process(stage string, wg *sync.WaitGroup, inCh chan int, outCh chan int) {
	defer wg.Done()
	for i := range inCh {
		// fmt.Println("Stage ", stage, " Got ", i)
		outCh <- mult2(i)
	}
	// fmt.Println("closing channel", outCh)
	close(outCh)
}

func mult2(in int) int {
	time.Sleep(1 * (time.Microsecond))
	return in * 2
}
