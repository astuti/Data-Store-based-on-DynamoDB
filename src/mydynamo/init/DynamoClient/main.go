package main

import (
	"fmt"
	"mydynamo"
	"strconv"
	"time"
)

func main() {
	//Spin up a client for some testing
	//Expects server to be started, starting at port 8080
	serverPort := 8080

	//this client connects to the server on port 8080
	clientInstance0 := mydynamo.NewDynamoRPCClient("localhost:" + strconv.Itoa(serverPort+0))
	clientInstance0.RpcConnect()
	
	clock1 := mydynamo.NewVectorClock()
	context1 := mydynamo.NewContext(clock1)
	putArgs1 := mydynamo.NewPutArgs("key1", context1, []byte("value1"))
	res0p := clientInstance0.Put(putArgs1)
	if res0p{
		fmt.Println("succ for put1")
	}else{
		fmt.Println("FAIL for put1")
	}
	res0g := clientInstance0.Get("key1")
	fmt.Println("res0g", res0g)
	//normal updating //desc is sent

	clock11 := mydynamo.CreateClock(map[string]int{"0": 1})
	context11 := mydynamo.NewContext(clock11)
	putArgs11 := mydynamo.NewPutArgs("key1", context11, []byte("value2"))
	res0p1 := clientInstance0.Put(putArgs11)
	if res0p1{
		fmt.Println("succ for put1 version2")
	}else{
		fmt.Println("FAIL for put1 version2")
	}
	res0g1 :=clientInstance0.Get("key1")
	fmt.Println("res0g1", res0g1)
	
	//non desc is sent
	clientInstance1 := mydynamo.NewDynamoRPCClient("localhost:" + strconv.Itoa(serverPort+0))
	clientInstance1.RpcConnect()
	clock2 := mydynamo.CreateClock(map[string]int{"0": 1})
	context2 := mydynamo.NewContext(clock2)
	putArgs2 := mydynamo.NewPutArgs("key1", context2, []byte("value1"))
	res2p := clientInstance1.Put(putArgs2)
	if res2p{
		fmt.Println("succ for put2 version1, non desc is sent")
	}else{
		fmt.Println("FAIL for put2 version1, non desc is sent")
	}

	res2g :=clientInstance1.Get("key1")
	fmt.Println("non desc get: res2g", res2g)
	//concurrent is sent
	clock3 := mydynamo.CreateClock(map[string]int{"0": 0, "1": 1})
	context3 := mydynamo.NewContext(clock3)
	putArgs3 := mydynamo.NewPutArgs("key1", context3, []byte("value3"))
	res3p := clientInstance1.Put(putArgs3)
	if res3p{
		fmt.Println("succ for put3 concurrent value3")
	}else{
		fmt.Println("FAIL for put3 concurrent value3")
	}
	res3g :=clientInstance1.Get("key1")
	fmt.Println("res3g", res3g)

	//equal is sent
	// clock3 := mydynamo.CreateClock(map[string]int{"0": 0, "1": 1})
	// context3 := mydynamo.NewContext(clock3)
	// putArgs3 := mydynamo.NewPutArgs("key1", context3, []byte("value3"))
	res4p := clientInstance1.Put(putArgs3)
	if res4p{
		fmt.Println("succ for put4 equal value3")
	}else{
		fmt.Println("FAIL for put4 equal value3")
	}
	res4g :=clientInstance1.Get("key1")
	fmt.Println("res4g", res4g)

	clientInstance2 := mydynamo.NewDynamoRPCClient("localhost:" + strconv.Itoa(serverPort+0))
	clientInstance2.RpcConnect()
	clientInstance2.Crash(10)
	
	cp1:= clientInstance0.Put(putArgs3)
	cg1:= clientInstance0.Get("key1")
	fmt.Println("cp1", cp1)
	fmt.Println("cg1", cg1)
	
	cp2:= clientInstance1.Put(putArgs3)
	cg2:= clientInstance1.Get("key1")
	fmt.Println("cp2", cp2)
	fmt.Println("cg2", cg2)

	time.Sleep(time.Duration(10) * time.Second)
	cp3:= clientInstance2.Put(putArgs3)
	cg3:= clientInstance2.Get("key1")
	fmt.Println("cp3", cp3)
	fmt.Println("cg3", cg3)
	//You can use the space below to write some operations that you want your client to do
}
