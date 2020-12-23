package mydynamotest

import (
	"mydynamo"
	"testing"
)

func TestBasicVectorClock(t *testing.T) {
	t.Logf("Starting TestBasicVectorClock")

	//create two vector clocks
	clock1 := mydynamo.NewVectorClock()
	clock2 := mydynamo.NewVectorClock()

	//Test for equality
	if !clock1.Equals(clock2) {
		t.Fail()
		t.Logf("Vector Clocks were not equal")
	}
}

func TestLessThanClock(t *testing.T) {
	t.Logf("Starting TestLessThanClock")

	//create two vector clocks
	c1Map := map[string]int{"node1": 1,
							"node2": 2} //1,2,0
	
	c2Map := map[string]int{"node1": 1,
	"node2": 3} //1,3,0 

	c3Map := map[string]int{"node1": 1,
	"node3": 3} //1,0,3

	clock1 := mydynamo.CreateClock(c1Map)
	clock2 := mydynamo.CreateClock(c2Map)
	clock3 := mydynamo.CreateClock(c3Map)
	//Returns true if the other VectorClock is causally descended from this one
	if !clock1.LessThan(clock2) {
		t.Fail()
		t.Logf("clock2 is descendant, but this didn't happen")
	}
	if clock2.LessThan(clock1) {
		t.Fail()
		t.Logf("clock2 is descendant, but clock1 is declared as descendant")
	}
	if clock3.LessThan(clock1){
		t.Fail()
		t.Logf("clock1 is declared descdendant to clock3, invalid")
	}
	if clock3.LessThan(clock2){
		t.Fail()
		t.Logf("clock2 is declared descdendant to clock3, invalid")
	}
}

func TestConcurrentClock(t *testing.T) {
	t.Logf("Starting TestConcurrentClock")

	//create two vector clocks
	c1Map := map[string]int{"node1": 1,
							"node2": 2} //1,2,0
	
	c2Map := map[string]int{"node1": 2,
	"node2": 2} //1,3,0 

	c3Map := map[string]int{"node1": 1,
	"node3": 3} //1,0,3

	clock1 := mydynamo.CreateClock(c1Map)
	clock2 := mydynamo.CreateClock(c2Map)
	clock3 := mydynamo.CreateClock(c3Map)
	//Returns true if the other VectorClock is causally descended from this one
	if clock1.Concurrent(clock2) {
		t.Fail()
		t.Logf("clock2 is descendant, but this didn't happen")
	}
	if clock2.Concurrent(clock1) {
		t.Fail()
		t.Logf("clock2 is descendant, but clock1 is declared as descendant")
	}
	if !clock3.Concurrent(clock1){
		t.Fail()
		t.Logf("clock1 is declared descdendant to clock3, invalid")
	}
	if !clock3.Concurrent(clock2){
		t.Fail()
		t.Logf("clock2 is declared descdendant to clock3, invalid")
	}
}

func TestEqualsClock(t *testing.T) {
	t.Logf("Starting TestEqualsClock")

	//create two vector clocks
	c1Map := map[string]int{"node1": 1,
	"node2": 2} 
	
	c2Map := map[string]int{"node1": 2,
	"node2": 1} 

	c3Map := map[string]int{"node1": 1,
	"node3": 1} 

	c4Map := map[string]int{"node1": 1,
	"node3": 1} 

	clock1 := mydynamo.CreateClock(c1Map)
	clock2 := mydynamo.CreateClock(c2Map)
	clock3 := mydynamo.CreateClock(c3Map)
	clock4 := mydynamo.CreateClock(c4Map)
	
	//Returns true if the other VectorClock is causally descended from this one
	if (&clock1).Equals(clock2) {
		t.Fail()
		t.Logf("clock1 can't be equal to clock2")
	}
	if (&clock2).Equals(clock3) {
		t.Fail()
		t.Logf("clock2 can't be equal to clock3")
	}
	if !(&clock3).Equals(clock4){
		t.Fail()
		t.Logf("clock3 is equal to clock4")
	}
}

func TestCombineClock(t *testing.T) {
	t.Logf("Starting TestCombineClock")

	//create two vector clocks
	c1Map := map[string]int{"node1": 1,
	"node2": 2} 
	
	c2Map := map[string]int{"node1": 2,
	"node2": 1} 

	s1 := map[string]int{"node1": 1,
	"node3": 1} 

	s2 := map[string]int{"node1": 1,
	"node2": 1} 
	s11 := map[string]int{"node1": 1,
	"node3": 1} 

	s22 := map[string]int{"node1": 1,
	"node2": 1} 
	s3 := map[string]int{} 

	clockC1 := mydynamo.CreateClock(c1Map)
	clockC2 := mydynamo.CreateClock(c2Map)
	clockS1 := mydynamo.CreateClock(s1)
	clockS2 := mydynamo.CreateClock(s2)
	clockS3 := mydynamo.CreateClock(s3)
	clockS11 := mydynamo.CreateClock(s11)
	clockS22 := mydynamo.CreateClock(s22)
	clocks := []mydynamo.VectorClock{clockC1, clockC2}
	clocksS := []mydynamo.VectorClock{clockS11, clockS22}
	(&clockS1).Combine(clocks)
	(&clockS2).Combine(clocks)
	(&clockS3).Combine(clocksS)

	correctS1 := map[string]int{"node1": 2,
	"node2": 2, "node3": 1} 

	correctS2 := map[string]int{"node1": 2,
	"node2": 2} 

	correctS3 := map[string]int{"node1": 1,
	"node2": 1, "node3": 1}	
	
	correctClockS1 :=  mydynamo.CreateClock(correctS1)
	correctClockS2 :=  mydynamo.CreateClock(correctS2)
	correctClockS3 :=  mydynamo.CreateClock(correctS3)
	
	if !(&clockS1).Equals(correctClockS1) {
		t.Fail()
		t.Logf("clock1 can't be concurrent to clock2")
	}
	if !(&clockS2).Equals(correctClockS2) {
		t.Fail()
		t.Logf("clock2 can't be concurrent to clock3")
	}
	if !(&clockS3).Equals(correctClockS3){
		t.Fail()
		t.Logf("clock3 is concurrent to clock4")
	}
}

func TestIncrementClock(t *testing.T) {
	t.Logf("Starting TestIncrementClock")

	//create two vector clocks
	c1Map := map[string]int{"node1": 1,
	"node2": 2} 
	clock1 :=mydynamo.CreateClock(c1Map)

	c2Map := map[string]int{"node1": 1,
	"node2": 2} 
	clock2 :=mydynamo.CreateClock(c2Map)

	(&clock1).Increment("node1")
	(&clock2).Increment("node3")

	r1Map := map[string]int{"node1": 2,
	"node2": 2}
	r2Map := map[string]int{"node1": 1,
	"node2": 2, "node3": 1} 
	correctRes1 := mydynamo.CreateClock(r1Map)
	correctRes2 := mydynamo.CreateClock(r2Map)
	
	//Returns true if the other VectorClock is causally descended from this one
	if !(&clock1).Equals(correctRes1) {
		t.Fail()
		t.Logf("res1 incorrect")
	}
	if !(&clock2).Equals(correctRes2) {
		t.Fail()
		t.Logf("res2 incorrect")
	}
}