package mydynamotest

import (
    "fmt"
    "mydynamo"
    "testing"
    "time"
)
//PASS
func TestPutW2(t *testing.T){
    t.Logf("Starting PutW2 test")
    cmd := InitDynamoServer("./twoserver.ini")
    ready := make(chan bool)
    go StartDynamoServer(cmd, ready)
    defer KillDynamoServer(cmd)

    time.Sleep(3 * time.Second)
    <-ready

    clientInstance0 := MakeConnectedClient(8080)
    clientInstance1 := MakeConnectedClient(8081)
    clientInstance0.Put(PutFreshContext("s1", []byte("abcde")))
    // gotValuePtr0 := clientInstance0.Get("s1")
    // if gotValuePtr0 == nil {
    //     t.Logf("TestPutW2: Failed to get from client0")
    // }

    gotValuePtr := clientInstance1.Get("s1")
    if gotValuePtr == nil {
        t.Fail()
        t.Logf("TestPutW2: Failed to get")
    }
    gotValue := *gotValuePtr
    if(len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde"))){
        t.Fail()
        t.Logf("TestPutW2: Failed to get value")
    }

}

func TestGossip(t *testing.T){ //PASS
    t.Logf("*****************Starting Gossip test")
    cmd := InitDynamoServer("./myconfig.ini")
    ready := make(chan bool)
    go StartDynamoServer(cmd, ready)
    defer KillDynamoServer(cmd)

    time.Sleep(3 * time.Second)
    <-ready

    clientInstance0 := MakeConnectedClient(8080)
    clientInstance1 := MakeConnectedClient(8081)
    clientInstance0.Put(PutFreshContext("s1", []byte("abcde")))
    t.Logf("Put client0 finished here, starting gossip")
    fmt.Println("=======FMT Put client0 finished here, starting gossip")
    clientInstance0.Gossip()
    gotValuePtr := clientInstance1.Get("s1")
    if gotValuePtr == nil {
        t.Fail()
        t.Logf("TestGossip: Failed to get, here at first")
        fmt.Println("======FINE VERY ONE")
    }
    gotValue := *gotValuePtr
    fmt.Println("======Value Read by Client1 =", gotValue)
    if (len(gotValue.EntryList) != 1){
        fmt.Println("======first condition true")
    }else{
        fmt.Println("======FINE 1")
    }
    if (!valuesEqual(gotValue.EntryList[0].Value, []byte("abcde"))){
        fmt.Println("=======second condition true")
    }else{
        fmt.Println("=======FINE 2")
    }
    if(len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde"))){
        t.Fail()
        t.Logf("TestGossip: Failed to get value")
    }

}
//PASS
func TestMultipleKeys(t *testing.T){
    t.Logf("Starting MultipleKeys test")
    cmd := InitDynamoServer("./myconfig.ini")
    ready := make(chan bool)
    go StartDynamoServer(cmd, ready)
    defer KillDynamoServer(cmd)

    time.Sleep(3 * time.Second)
    <-ready

    clientInstance0 := MakeConnectedClient(8080)
    clientInstance1 := MakeConnectedClient(8081)
    clientInstance0.Put(PutFreshContext("s1", []byte("abcde")))
    clientInstance0.Gossip()
    gotValuePtr := clientInstance0.Get("s1")
    if gotValuePtr == nil {
        t.Fail()
        t.Logf("TestMultipleKeys: Failed to get")
    }
    gotValue := *gotValuePtr
    fmt.Println("Got value ======", gotValue)
    if (len(gotValue.EntryList) != 1 ){
        fmt.Print("========== one true")
    }else{
        fmt.Print("========== one false")
    }
    if (!valuesEqual(gotValue.EntryList[0].Value, []byte("abcde"))){
        fmt.Print("========== second true")
    }else{
        fmt.Print("========== second false")
    }
    if(len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde"))){
        t.Fail()
        t.Logf("TestMultipleKeys: Failed to get value")
    }

    gotValuePtr = clientInstance1.Get("s1")
    if gotValuePtr == nil {
        fmt.Print("========== gotValuePtr nil")
        t.Fail()
        t.Logf("TestMultipleKeys: Failed to get")
    }
    gotValue = *gotValuePtr

    clientInstance1.Put(mydynamo.NewPutArgs("s1",gotValue.EntryList[0].Context ,[]byte("efghi")))
    gotValuePtr = clientInstance1.Get("s1")
    if gotValuePtr == nil {
        t.Fail()
        t.Logf("TestMultipleKeys: Failed to get")
    }
    gotValue = *gotValuePtr
    if(len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("efghi"))){
        t.Fail()
        t.Logf("TestMultipleKeys: Failed to get value")
    }
}
//PASS
func TestDynamoPaper(t *testing.T){
    t.Logf("DynamoPaper test")
    cmd := InitDynamoServer("./myconfig.ini")
    ready := make(chan bool)
    go StartDynamoServer(cmd, ready)
    defer KillDynamoServer(cmd)

    time.Sleep(3 * time.Second)
    <-ready

    clientInstance0 := MakeConnectedClient(8080)
    clientInstance1 := MakeConnectedClient(8081)
    clientInstance2 := MakeConnectedClient(8082)

    clientInstance0.Put(PutFreshContext("s1", []byte("abcde")))
    gotValuePtr := clientInstance0.Get("s1")
    if gotValuePtr == nil {
        t.Fail()
        t.Logf("TestDynamoPaper: Failed to get first value")
    }

    gotValue := *gotValuePtr
    if(len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde"))){
        t.Fail()
        t.Logf("TestDynamoPaper: First value doesn't match")
    }
    clientInstance0.Put(mydynamo.NewPutArgs("s1", gotValue.EntryList[0].Context, []byte("bcdef")))
    gotValuePtr = clientInstance0.Get("s1")
    if gotValuePtr == nil {
        t.Fail()
        t.Logf("TestDynamoPaper: Failed to get second value")
    }
    gotValue = *gotValuePtr
    if(len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("bcdef"))){
        //version 2 (0:1)
        t.Fail()
        t.Logf("TestDynamoPaper: Second value doesn't match")
    }

    clientInstance0.Gossip()// version 2 on other servers
    clientInstance1.Put(mydynamo.NewPutArgs("s1", gotValue.EntryList[0].Context, []byte("cdefg")))
    clientInstance2.Put(mydynamo.NewPutArgs("s1", gotValue.EntryList[0].Context, []byte("defgh")))
    gotValuePtr = clientInstance1.Get("s1")
    if gotValuePtr == nil {
        t.Fail()
        t.Logf("TestDynamoPaper: Failed to get third value")
    }
    gotValue = *gotValuePtr //version (0:2, 1:1)
    fmt.Println("@@@@@@@@@@@@@@@@@ Third Value", gotValue)
    fmt.Println("@@@@@@@@@@@@@@@@@ Third Value", string(gotValue.EntryList[0].Value))
    
    if(len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("cdefg"))){
        t.Fail()
        t.Logf("TestDynamoPaper: Third value doesn't match")
    }
    gotValuePtr = clientInstance2.Get("s1")
    fmt.Println("@@@@@@@@@@@@@@@@@ Fourth Value", gotValuePtr)
    fmt.Println("@@@@@@@@@@@@@@@@@ Fourth Value", string(gotValuePtr.EntryList[0].Value))
    if gotValuePtr == nil {
        t.Fail()
        t.Logf("TestDynamoPaper: Failed to get fourth value")
    }
    gotValue = *gotValuePtr
    fmt.Println("FFFF orth value", gotValue)
    if(len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("defgh"))){
        t.Fail()
        t.Logf("TestDynamoPaper: Fourth value doesn't match")
    }
    clientInstance1.Gossip()
    clientInstance2.Gossip()
    gotValuePtr = clientInstance0.Get("s1")
    if gotValuePtr == nil {
        t.Fail()
        t.Logf("TestDynamoPaper: Failed to get fifth value")
    }
    gotValue = *gotValuePtr
    clockList := make([]mydynamo.VectorClock, 0)
    for _, a := range gotValue.EntryList {
        clockList = append(clockList, a.Context.Clock)
    }
    clockList[0].Combine(clockList)
    combinedClock := clockList[0]
    combinedContext := mydynamo.Context {
        Clock:combinedClock,
    }
    clientInstance0.Put(mydynamo.NewPutArgs("s1", combinedContext, []byte("zyxw")))
    gotValuePtr = clientInstance0.Get("s1")
    if gotValuePtr == nil {
        t.Fail()
        t.Logf("TestDynamoPaper: Failed to get sixth value")
    }
    gotValue = *gotValuePtr
    if(len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("zyxw"))){
        t.Fail()
        t.Logf("TestDynamoPaper: Sixth value doesn't match")
    }

}
//PASS
func TestInvalidPut(t *testing.T){
    t.Logf("Starting repeated Put test")
    cmd := InitDynamoServer("./myconfig.ini")
    ready := make(chan bool)
    go StartDynamoServer(cmd, ready)
    defer KillDynamoServer(cmd)

    time.Sleep(3 * time.Second)
    <-ready
    clientInstance := MakeConnectedClient(8080)

    clientInstance.Put(PutFreshContext("s1", []byte("abcde")))
    clientInstance.Put(PutFreshContext("s1", []byte("efghi")))
    gotValue := clientInstance.Get("s1")
    if(len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde"))){
        t.Fail()
        t.Logf("TestInvalidPut: Got wrong value")
    }
}
//PASS
func TestGossipW2(t *testing.T){
    t.Logf("******************Starting GossipW2 test")
    cmd := InitDynamoServer("./twoserver.ini")
    ready := make(chan bool)
    go StartDynamoServer(cmd, ready)
    defer KillDynamoServer(cmd)

    time.Sleep(3 * time.Second)
    <-ready

    clientInstance0 := MakeConnectedClient(8080)
    clientInstance1 := MakeConnectedClient(8081)
    clientInstance0.Put(PutFreshContext("s1", []byte("abcde")))
    clientInstance0.Gossip()
    gotValuePtr := clientInstance1.Get("s1")
    if gotValuePtr == nil {
        t.Fail()
        t.Logf("TestGossipW2: Failed to get first element")
    }
    gotValue := *gotValuePtr
    if(len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde"))){
        t.Fail()
        t.Logf("TestGossipW2: Failed to get value")
    }
    clientInstance1.Put(mydynamo.NewPutArgs("s1",gotValue.EntryList[0].Context, []byte("efghi")))

    gotValuePtr = clientInstance1.Get("s1")
    if gotValuePtr == nil {
        t.Fail()
        t.Logf("TestGossipW2: Failed to get")
    }
    gotValue = *gotValuePtr

    if(len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("efghi"))){
        t.Fail()
        t.Logf("GossipW2: Failed to get value")
    }
    gotValuePtr = clientInstance0.Get("s1")
    if gotValuePtr == nil {
        t.Fail()
        t.Logf("TestGossipW2: Failed to get")
    }
    gotValue = *gotValuePtr

    if(len(gotValue.EntryList) != 1) || !valuesEqual(gotValue.EntryList[0].Value, []byte("efghi")){
        t.Fail()
        t.Logf("GossipW2: Failed to get value")
    }

}
//PASS
func TestReplaceMultipleVersions(t *testing.T){
    t.Logf("Starting ReplaceMultipleVersions test")
    cmd := InitDynamoServer("./myconfig.ini")
    ready := make(chan bool)
    go StartDynamoServer(cmd, ready)
    defer KillDynamoServer(cmd)

    time.Sleep(3 * time.Second)
    <-ready

    clientInstance0 := MakeConnectedClient(8080)
    clientInstance1 := MakeConnectedClient(8081)
    clientInstance0.Put(PutFreshContext("s1", []byte("abcde")))
    clientInstance1.Put(PutFreshContext("s1", []byte("efghi")))
    clientInstance0.Gossip()
    gotValuePtr := clientInstance1.Get("s1")
    if gotValuePtr == nil {
        t.Fail()
        t.Logf("TestReplaceMultipleVersions: Failed to get")
    }

    gotValue := *gotValuePtr
    clockList := make([]mydynamo.VectorClock, 0)
    for _, a := range gotValue.EntryList {
        clockList = append(clockList, a.Context.Clock)
    }
    clockList[0].Combine(clockList)
    combinedClock := clockList[0]
    combinedContext := mydynamo.Context {
        Clock:combinedClock,
    }
    clientInstance1.Put(mydynamo.NewPutArgs("s1", combinedContext, []byte("zxyw")))
    gotValuePtr = nil
    gotValuePtr = clientInstance1.Get("s1")
    if gotValuePtr == nil {
        t.Fail()
        t.Logf("TestReplaceMultipleVersions: Failed to get")
    }


    gotValue = *gotValuePtr
    if(len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("zxyw"))){
        t.Fail()
        t.Logf("testReplaceMultipleVersions: Values don't match")
    }


}
//PASS
func TestConsistent(t *testing.T){
    t.Logf("Starting Consistent test")
    cmd := InitDynamoServer("./consistent.ini")
    ready := make(chan bool)
    go StartDynamoServer(cmd, ready)
    defer KillDynamoServer(cmd)

    time.Sleep(3 * time.Second)
    <-ready

    clientInstance0 := MakeConnectedClient(8080)
    clientInstance1 := MakeConnectedClient(8081)
    clientInstance2 := MakeConnectedClient(8082)
    clientInstance3 := MakeConnectedClient(8083)
    clientInstance4 := MakeConnectedClient(8084)

    clientInstance0.Put(PutFreshContext("s1", []byte("abcde")))
    gotValuePtr := clientInstance1.Get("s1")
    if gotValuePtr == nil {
        t.Fail()
        t.Logf("TestConsistent: Failed to get")
    }
    gotValue := *gotValuePtr
    fmt.Println("got Value", gotValue)
    if(len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde"))){
        t.Fail()
        t.Logf("TestConsistent: Failed to get value")
    }


    clientInstance3.Put(mydynamo.NewPutArgs("s1", gotValue.EntryList[0].Context, []byte("zyxw")))
    clientInstance0.Crash(3)
    clientInstance1.Crash(3)
    clientInstance4.Crash(3)
    // time.Sleep(10*time.Second)
    
    gotValuePtr = clientInstance2.Get("s1")
    if gotValuePtr == nil {
        t.Fail()
        t.Logf("TestConsistent: Failed to get")
    }
    gotValue = *gotValuePtr
    if(len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("zyxw"))){
        t.Fail()
        t.Logf("TestConsistent: Failed to get value")
    }
}
//Test our 1
func TestOur1(t *testing.T){
    t.Logf("Starting Our test 1")
    cmd := InitDynamoServer("./twoserver.ini")
    ready := make(chan bool)
    go StartDynamoServer(cmd, ready)
    defer KillDynamoServer(cmd)

    time.Sleep(3 * time.Second)
    <-ready

    clientInstance0 := MakeConnectedClient(8080)
    clientInstance1 := MakeConnectedClient(8081)
    clientInstance2 := MakeConnectedClient(8082)
    clientInstance3 := MakeConnectedClient(8083)
    clientInstance4 := MakeConnectedClient(8084)
    clientInstance2.Crash(5)
    clientInstance0.Put(PutFreshContext("s1", []byte("a")))
    clientInstance3.Put(PutFreshContext("s1", []byte("b")))
    time.Sleep(10 * time.Second) //so that 2 recovers
    gotValuePtr := clientInstance1.Get("s1")
    if gotValuePtr == nil {
        t.Fail()
        t.Logf("OurTest1: Failed to get")
    }
    gotValue := *gotValuePtr
    fmt.Println("@@@@@@@@@@@@@@@@@ Our Test Client1 Value", gotValue)
    fmt.Println("@@@@@@@@@@@@@@@@@ Our Test Client1 Value", string(gotValue.EntryList[0].Value))
    if(len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("a"))){
        t.Fail()
        t.Logf("OurTest1: Failed to get value")
    }
    gotValuePtr = clientInstance4.Get("s1")
    if gotValuePtr == nil {
        t.Fail()
        t.Logf("OurTest1: Failed to get from Client4")
    }
    gotValue = *gotValuePtr
    fmt.Println("@@@@@@@@@@@@@@@@@ Our Test Client4 Value", gotValue)
    // fmt.Println("@@@@@@@@@@@@@@@@@ Our Test Client4 Value", string(gotValue.EntryList[0].Value))
}

//Test Our 2
func TestOur2(t *testing.T){
    t.Logf("Starting Our test 2, for R=5, W=5")
    cmd := InitDynamoServer("./allconfig.ini")
    ready := make(chan bool)
    go StartDynamoServer(cmd, ready)
    defer KillDynamoServer(cmd)

    time.Sleep(3 * time.Second)
    <-ready

    clientInstance0 := MakeConnectedClient(8080)
    clientInstance1 := MakeConnectedClient(8081)
    clientInstance2 := MakeConnectedClient(8082)
    clientInstance3 := MakeConnectedClient(8083)
    clientInstance4 := MakeConnectedClient(8084)

    clientInstance0.Put(PutFreshContext("s1", []byte("a")))
    clientInstance3.Put(PutFreshContext("s1", []byte("b")))
 
    gotValuePtr := clientInstance1.Get("s1")
    gotValue := *gotValuePtr
    fmt.Println("@@@@@@@@@@@@@@@@@ 1", gotValue)
    // fmt.Println("@@@@@@@@@@@@@@@@@ 1", string(gotValue.EntryList[0].Value))

    gotValuePtr = clientInstance4.Get("s1")
    gotValue = *gotValuePtr
    fmt.Println("@@@@@@@@@@@@@@@@@ 2", gotValue)

    clientInstance1.Put(PutFreshContext("s1", []byte("c")))
    clientInstance4.Put(PutFreshContext("s1", []byte("d")))

    gotValuePtr = clientInstance1.Get("s1")
    gotValue = *gotValuePtr
    fmt.Println("@@@@@@@@@@@@@@@@@ 3", gotValue)
    // fmt.Println("@@@@@@@@@@@@@@@@@ 3", string(gotValue.EntryList[0].Value))

    gotValuePtr = clientInstance4.Get("s1")
    gotValue = *gotValuePtr
    fmt.Println("@@@@@@@@@@@@@@@@@ 4", gotValue)

    gotValuePtr = clientInstance2.Get("s1")
    gotValue = *gotValuePtr
    fmt.Println("@@@@@@@@@@@@@@@@@ 5", gotValue)

    gotValuePtr = clientInstance0.Get("s1")
    gotValue = *gotValuePtr
    fmt.Println("@@@@@@@@@@@@@@@@@ 6", gotValue)

    gotValuePtr = clientInstance3.Get("s1")
    gotValue = *gotValuePtr
    fmt.Println("@@@@@@@@@@@@@@@@@ 7", gotValue)
}

func TestOur3(t *testing.T){
    t.Logf("Starting Our test 3, for R=1, W=1")
    cmd := InitDynamoServer("./myconfig.ini")
    ready := make(chan bool)
    go StartDynamoServer(cmd, ready)
    defer KillDynamoServer(cmd)

    time.Sleep(3 * time.Second)
    <-ready

    clientInstance0 := MakeConnectedClient(8080)
    clientInstance1 := MakeConnectedClient(8081)
    clientInstance2 := MakeConnectedClient(8082)
    clientInstance3 := MakeConnectedClient(8083)
    clientInstance4 := MakeConnectedClient(8084)

    // clientInstance0.Put(PutFreshContext("s1", []byte("a")))
    // clientInstance3.Put(PutFreshContext("s1", []byte("b")))
 
    gotValuePtr := clientInstance1.Get("s1")
    gotValue := *gotValuePtr
    fmt.Println("@@@@@@@@@@@@@@@@@ 1", gotValue)
    // fmt.Println("@@@@@@@@@@@@@@@@@ 1", string(gotValue.EntryList[0].Value))

    gotValuePtr = clientInstance4.Get("s1")
    gotValue = *gotValuePtr
    fmt.Println("@@@@@@@@@@@@@@@@@ 2", gotValue)

    clientInstance1.Put(PutFreshContext("s1", []byte("c")))
    clientInstance4.Put(PutFreshContext("s1", []byte("d")))

    gotValuePtr = clientInstance1.Get("s1")
    gotValue = *gotValuePtr
    fmt.Println("@@@@@@@@@@@@@@@@@ 3", gotValue)
    // fmt.Println("@@@@@@@@@@@@@@@@@ 3", string(gotValue.EntryList[0].Value))

    gotValuePtr = clientInstance4.Get("s1")
    gotValue = *gotValuePtr
    fmt.Println("@@@@@@@@@@@@@@@@@ 4", gotValue)

    gotValuePtr = clientInstance2.Get("s1")
    gotValue = *gotValuePtr
    fmt.Println("@@@@@@@@@@@@@@@@@ 5", gotValue)
    gotValuePtr = clientInstance3.Get("s1")
    gotValue = *gotValuePtr
    fmt.Println("@@@@@@@@@@@@@@@@@ 6", gotValue)
    gotValuePtr = clientInstance0.Get("s1")
    gotValue = *gotValuePtr
    fmt.Println("@@@@@@@@@@@@@@@@@ 7", gotValue)

    clientInstance1.Gossip()
    clientInstance4.Gossip()
    fmt.Println("@@@@@@@@@@@@ AFTER GOSSIP")
    gotValuePtr = clientInstance0.Get("s1")
    gotValue = *gotValuePtr
    fmt.Println("@@@@@@@@@@@@@@@@@ 8", gotValue)

    gotValuePtr = clientInstance1.Get("s1")
    gotValue = *gotValuePtr
    fmt.Println("@@@@@@@@@@@@@@@@@ 9", gotValue)
    gotValuePtr = clientInstance2.Get("s1")
    gotValue = *gotValuePtr
    fmt.Println("@@@@@@@@@@@@@@@@@ 10", gotValue)
    gotValuePtr = clientInstance3.Get("s1")
    gotValue = *gotValuePtr
    fmt.Println("@@@@@@@@@@@@@@@@@ 11", gotValue)
    gotValuePtr = clientInstance4.Get("s1")
    gotValue = *gotValuePtr
    fmt.Println("@@@@@@@@@@@@@@@@@ 12", gotValue)
    
}
