package mydynamo

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
	"net"
	"net/http"
	"net/rpc"
)

var m sync.Mutex

type DynamoServerClient struct {
	ServerAddr string
	rpcConn    *rpc.Client
}
// var mutex = &sync.Mutex{}
func NewDynamoServerClient(serverAddr string) *DynamoServerClient {
	return &DynamoServerClient{
		ServerAddr: serverAddr,
		rpcConn:    nil,
	}
}
func (dynamoServerClient *DynamoServerClient) ConnDynamoServer() error {
	if dynamoServerClient.rpcConn != nil {
		return nil
	}
	var e error
	fmt.Println("other server Addr", dynamoServerClient.ServerAddr)
	dynamoServerClient.rpcConn, e = rpc.DialHTTP("tcp", dynamoServerClient.ServerAddr)
	if e != nil {
		fmt.Println("Error in Dialing other put server local", e)
		dynamoServerClient.rpcConn = nil
	}
	return e
}

//wrapper function to call put rpc func
func (dynamoServerClient *DynamoServerClient) PutServerWrapper(value PutArgs) bool {
	fmt.Println(".....Put Server Wrapper Called.....")
	var result bool
	if dynamoServerClient.rpcConn == nil {
		fmt.Println("no live connection with other server")
		return false
	}
	fmt.Println("Calling other servers Put Local")
	//todo: check this again Put Local 
	err := dynamoServerClient.rpcConn.Call("MyDynamo.PutLocalNoIncrement", value, &result)
	if err != nil {
		log.Println(err)
		return false
	}
	return result
}

//wrapper function to call rpc funcs
func (dynamoServerClient *RPCClient) GossipServerWrapper(value PutArgs) bool {
	fmt.Println(".....Gossip Server Wrapper Called.....")
	var result bool
	if dynamoServerClient.rpcConn == nil {
		return false
	}
	err := dynamoServerClient.rpcConn.Call("MyDynamo.GossipServer", value, &result)
	if err != nil {
		log.Println(err)
		return false
	}
	return result
}
// //rpc functions for server communication
// func (s *DynamoServer) PutServer(value PutArgs, result *bool) error {

// }
//rpc functions for server communication
func GossipServer() { //todo: same time removal or later, todo: add error return?
	//to all 
	//don't copy if already copied by put
	fmt.Println(".....Gossip Server Started.....")
	var toRemove []int
	// fmt.Println("Inside Gossip Server")
	fmt.Print("Gossip Data=", GossipData)
	for idx, datum := range GossipData{
		// nodeIdToConnect := datum.//todo: get nodeID/dynamo Server
		// dynamoServerToConnect := datum.ToServerAddr// get nodeID/dynamo Server
		serverClientInstance := NewDynamoServerClient(datum.ToServerAddr)
		serverClientInstance.ConnDynamoServer()
		value := PutArgs{
			Key     : datum.Key,
			Context : datum.Context,
			Value   : datum.Value,	
		}
		succ := serverClientInstance.PutServerWrapper(value)
		if succ{
			toRemove = append(toRemove, idx)
			//remove fromlist
			//just add index to an array and remove later
		}else{
			//record failure and update gossipData
			//do nothing here
		}
	}

	GossipData = removeGossipData(GossipData, toRemove)
	// for _, idx := range toRemove{
	// 	GossipData = removeGossipData(GossipData, idx)
	// } 
	fmt.Println("Final Gossip Data left", GossipData)
}
var GossipData []GossipDatum //similar datamap is used for gossip as well, because different put will have different state

type GossipDatum struct {
	ToServerAddr string
	// nodeID string
	Key string
	Context Context
	Value   []byte
}
//assumed put will only get single clock thing ??? 
//gossip is only outgoing
//make separate function for gossip
//prepare a function for internal communication
//* where is combine operation used- user will only use.. 
//gossip update both the values, put does only one it gets
//vector clocks that should be (node, version, value)- same value issue resolve
//list of context (are context associated with values?)
//new context after increment equal and before increment equal cases
//for example if we used a hash map for key -> (value, context), then we will need to have key -> list of (value, context)

func (s *DynamoServer) accessGossipData(){

	m.Lock()
	//put
	GossipServer()
	m.Unlock()
}
func (s *DynamoServer) accessGossipDataForReplication(value PutArgs) error { //keep a separate function for gossip
	//todo: check mutex thing
	if (*s).crashState{
		return errors.New("Node crashed")		
	}
	m.Lock()
	//put
	err := (*s).ReplicatePut(value)// get PUtArgs here
		//todo: put a check if value is empty
	if err!=nil{
		panic("replicate put failed for local node")
	}
	m.Unlock()
	return err
}

func (s *DynamoServer) SelfCheck(dNode DynamoNode) bool {
	 //selfNode       DynamoNode  
	if ((*s).selfNode.Address == dNode.Address && (*s).selfNode.Port == dNode.Port){
		return true
	}else{
		return false
	}
}

// type DynamoNode struct {
// 	Address string
// 	Port    string
// }


func (s *DynamoServer) ReplicatePut(value PutArgs) error{ //todo: error here? 
	if (*s).crashState{
		return errors.New("Node crashed")		
	}
	fmt.Println(".....Replicating Put Now.....")
	fmt.Println("Preference list", (*s).preferenceList)
	counter := 0
	succCnt := 0
	wValue := (*s).wValue
	// fmt.Println("succCnt", succCnt)
	// fmt.Println("wvalue", wValue)
	// fmt.Println("counter", counter)
	// fmt.Println("len preference list",len((*s).preferenceList))
	//take top w-1  
	for ;(succCnt<(wValue-1)) && (counter<len((*s).preferenceList)); counter++{ //todo: check the condition again
		//call put on the server
		// fmt.Println("ReplicatePut: inside counter for loop")
		dNode := (*s).preferenceList[counter]
		if (*s).SelfCheck(dNode){ //skip if self node in preference list
			continue
		}
		serverAddr := dNode.Address+ ":" + dNode.Port
		serverClientInstance := NewDynamoServerClient(serverAddr)
		serverClientInstance.ConnDynamoServer()
		// fmt.Println("Calling Put Server Wrapper")
		succ := serverClientInstance.PutServerWrapper(value)//should return succ on when not crashed
		if succ{
			succCnt += 1
			fmt.Println("Successfully Replicated to ", serverAddr)
		}else{
			//record failure and update gossipData
			gossipDatum := GossipDatum{
				ToServerAddr: serverAddr,
				// nodeID: (*s).nodeID,
				Key: value.Key,
				Context: value.Context,
				Value:   value.Value,
			}
			fmt.Println("Failed Replication for ", serverAddr, "adding to Gossip Data")
			// otherNodeID //(nodeID, key)[`key`] and then (Context,Value)[`Value`]
			GossipData = append(GossipData,gossipDatum) // check what all will be transferred.
		}
		//todo: disconnect here
	}
	fmt.Println("Replicate Put, now adding remaining and crashed to Gossip Data")
	for ;(counter<len((*s).preferenceList)); counter++{
		dNode := (*s).preferenceList[counter]
		if (*s).SelfCheck(dNode){ //skip if self node in preference list
			continue
		}
		serverAddr := dNode.Address+ ":" + dNode.Port
		gossipDatum := GossipDatum{
			ToServerAddr: serverAddr,
			// nodeID: (*s).nodeID,
			Key: value.Key,
			Context: value.Context,
			Value:   value.Value,
		}
		fmt.Println(">>Replicate Put, add ", serverAddr, "to Gossip Data")
		GossipData = append(GossipData,gossipDatum) 
	}
	return nil
}

func (s *DynamoServer)startCrash(seconds int) {
	fmt.Println("Crash Starts Now")
	(*s).crashState = true
	time.Sleep(time.Duration(seconds)* time.Second) 
	(*s).crashState = false
	fmt.Println("Crash Ended")
    //sleep
}

type DynamoServer struct {
	/*------------Dynamo-specific-------------*/
	wValue         int          //Number of nodes to write to on each Put
	rValue         int          //Number of nodes to read from on each Get
	preferenceList []DynamoNode //Ordered list of other Dynamo nodes to perform operations o
	selfNode       DynamoNode   //This node's address and port info
	crashState	   bool
	nodeID         string       //ID of this node
	mapDataTuple   map[string][]ObjectEntry  //key, key; value: context + value (DataTuple)
	// nodeVectorClock int
}

func (s *DynamoServer) SendPreferenceList(incomingList []DynamoNode, _ *Empty) error {
	s.preferenceList = incomingList
	return nil
}

// Forces server to gossip
// As this method takes no arguments, we must use the Empty placeholder

//func (s *DynamoServer) accessGossipData(ReplicateOrGossip bool, value PutArgs ){
func (s *DynamoServer) Gossip(_ Empty, _ *Empty) error { //todo: returning nil only
	if (*s).crashState{
		return errors.New("Node crashed")		
	}
	// fmt.Println("Inside Gossip RPC call")
	(*s).accessGossipData() //todo: check here if this is fine
	return nil
	// panic("todo")
}

//Makes server unavailable for some seconds
func (s *DynamoServer) Crash(seconds int, success *bool) error {
	fmt.Println(".....Crashing server.....", (*s).selfNode)
	go (*s).startCrash(seconds)
	*success = true //todo: should we set success here
	return nil //todo: should we check erro in go routine
	// Shouldn't need to check crashState.
}

// type PutArgs struct {
// 	Key     string
// 	Context Context
// 	Value   []byte
// }

func (s *DynamoServer) Put(value PutArgs, result *bool) error {
	//todo: call put local 
	//replication
	// mutex.Lock()
	fmt.Println("....Starting Put....", value, "for server", (*s).selfNode)
	if (*s).crashState{
		(*result) = false
		return errors.New("Node crashed")		
	}

	value.Context.Clock.PairClock[(*s).nodeID] = value.Context.Clock.PairClock[(*s).nodeID]+1
	
	var resultLocal bool
	err := (*s).PutLocal(value, &resultLocal) //not a rpc call
	if (err !=nil || !resultLocal){
		(*result) = false
		return errors.New("Put Local Error")	
	}
	fmt.Println("....Finished PutLocal....")
	// wValue (*s).preferenceList
	//check if local put was success
	// mutex.Unlock()
	err = (*s).accessGossipDataForReplication(value) //todo: check -> this is always going to return nil..
	if err !=nil{
		(*result) = false	
	}
	return err
}

// Put a file to this server and W other servers
func (s *DynamoServer) PutLocalNoIncrement(value PutArgs, result *bool) error {
	// mutex.Lock()
	if (*s).crashState{
		(*result) = false
		return errors.New("Node crashed")		
	}
	fmt.Println(".....Put LocalNoIncrement.....")
	// 1. if old(this) context is already causally descendant to new one, fail
	newClock := value.Context.Clock
	// newClock.PairClock[(*s).nodeID] = newClock.PairClock[(*s).nodeID]//+1 
	key := value.Key
	newContext := Context{Clock: newClock}
	newObjectEntry := ObjectEntry{Context: newContext, Value: value.Value}
	// fmt.Println("yes, new Object Entry received in Put", newObjectEntry)
	// fmt.Println("new Clock", newClock)
	//get combined clock here
	// existingClock := VectorClock{}
	// listVectorClocks := []VectorClock{}
	// for _, objEntry := range (*s).mapDataTuple[key]{
	// 	listVectorClocks = append(listVectorClocks, objEntry.Context.Clock)
	// }
	// existingClock.Combine(listVectorClocks)

	// ???: will get discarded even if it's equal, should we fail here? 
	countCurrDesc := 0
	newListOjectEntry := []ObjectEntry{}
	foundEqual := false
	for _, objEntry := range (*s).mapDataTuple[key]{
		fmt.Println("-----------------------------")
		fmt.Println(">>Put LocalNoIncrement.... checking objEntry", objEntry)
		vc := objEntry.Context.Clock
		currIsDesc := newClock.LessThan(vc)
		if currIsDesc{
			fmt.Println(">>current is Desc of this OBJ")
			newListOjectEntry = append(newListOjectEntry, objEntry) //don't append here
			countCurrDesc += 1
			continue
		}
		newIsConcurrent := newClock.Concurrent(vc)
		if newIsConcurrent{
			fmt.Println(">>new is Concurrent")
			newListOjectEntry = append(newListOjectEntry, objEntry) //append here
			continue
		}
		newIsDesc := vc.LessThan(newClock)
		if newIsDesc{
			fmt.Println(">>new is Descendant")
			//don't add vc to new list. will have to append newClock
			continue
		}
		newIsEqual := (&newClock).Equals(vc)
		if newIsEqual{
			fmt.Println(">>new is Equal")
			foundEqual = true
			newListOjectEntry = append(newListOjectEntry, objEntry)
			// don't append here
			continue
		}
	}
	if ((countCurrDesc > 0) && (countCurrDesc == len((*s).mapDataTuple[key]))){
		//dont append
	}else if foundEqual{
		//don't append
	}else{
		newListOjectEntry = append(newListOjectEntry, newObjectEntry)
		// fmt.Println("appending to newListObjectEntry")
	}

	//todo: check what failures conditions are here.
	fmt.Println("PutLocalNoIncrement, node=",(*s).nodeID," key=", key,"stored(put)=",newListOjectEntry)
	// newObjectEntry := ObjectEntry{Context: value.Context,
	// 		Value: value.Value,}
	// currListObjectEntry := (*s).mapDataTuple[key] //??? check assignments here
	// currListObjectEntry = append(currListObjectEntry, newObjectEntry)
	
	(*s).mapDataTuple[key] = newListOjectEntry
	// mutex.Unlock()
	(*result) = true
	// currIsDesc := newClock.LessThan(existingClock)
	// if currIsDesc{
	// 	//fail here
	// 	(*result) = false // ??? should we set false here if value is not updated?
	// 	//??? should we set to w other nodes here as well?  
	// 	return errors.New("Put failed")
	// }
	// //1.1 if new is descendant
	// newIsDesc := existingClock.LessThan(newClock)
	// if newIsDesc{
	// 	(*result) = true
	// 	//replace the old one
	// 	newContext := value.Context
	// 	newContext.Clock.PairClock[(*s).nodeID] = newContext.Clock.PairClock[(*s).nodeID]+1 
	// 	newObjectEntry := ObjectEntry{Context: newContext,
	// 		Value: value.Value,}
	// 	(*s).mapDataTuple[key] = []ObjectEntry{newObjectEntry}
	// 	// return nil
	// 	// todo: complete this
	// }

	// //1.2. If concurrent, store both the values? 
	// newIsConcurrent := existingClock.Concurrent(newClock)
	// if newIsConcurrent{
	// 	newObjectEntry := ObjectEntry{Context: value.Context,
	// 									Value: value.Value,}
	// 	currListObjectEntry := (*s).mapDataTuple[key] //??? check assignments here
	// 	currListObjectEntry = append(currListObjectEntry, newObjectEntry)
	// 	(*s).mapDataTuple[key] = currListObjectEntry
	// 	// return nil
	// }
	//replication
	//2. todo: replication
	//3. Increment the vector clock 
	//??? should increment here ? if 
	//4. todo: Store nodes for future gossip operation 
	// panic("todo")
	//***** replication using put

	//***** 
	
	return nil
}
func (s *DynamoServer) PutLocal(value PutArgs, result *bool) error {
	if (*s).crashState{
		(*result) = false
		return errors.New("Node crashed")		
	}
	fmt.Println(".....Starting PutLocal......")
	// 1. if old(this) context is already causally descendant to new one, fail
	newClock := value.Context.Clock
	// newClock.PairClock[(*s).nodeID] = newClock.PairClock[(*s).nodeID]+1  //INCrement not needed here
	key := value.Key
	newContext := Context{Clock: newClock}
	newObjectEntry := ObjectEntry{Context: newContext, Value: value.Value}
	// fmt.Println("yes, new Object Entry received in Put", newObjectEntry)
	// fmt.Println("new Clock", newClock)
	//get combined clock here
	// existingClock := VectorClock{}
	// listVectorClocks := []VectorClock{}
	// for _, objEntry := range (*s).mapDataTuple[key]{
	// 	listVectorClocks = append(listVectorClocks, objEntry.Context.Clock)
	// }
	// existingClock.Combine(listVectorClocks)

	// ???: will get discarded even if it's equal, should we fail here? 
	countCurrDesc := 0
	newListOjectEntry := []ObjectEntry{}
	foundEqual := false
	for _, objEntry := range (*s).mapDataTuple[key]{
		fmt.Println("----------------------------")
		fmt.Println(">>checking objEntry", objEntry)
		vc := objEntry.Context.Clock
		currIsDesc := newClock.LessThan(vc)
		if currIsDesc{
			fmt.Println("current is Desc of this OBJ")
			newListOjectEntry = append(newListOjectEntry, objEntry) //don't append here
			countCurrDesc += 1
			continue
		}
		newIsConcurrent := newClock.Concurrent(vc)
		if newIsConcurrent{
			fmt.Println("new is Concurrent")
			newListOjectEntry = append(newListOjectEntry, objEntry) //append here
			continue
		}
		newIsDesc := vc.LessThan(newClock)
		if newIsDesc{
			fmt.Println("new is Descendant")
			//don't add vc to new list. will have to append newClock
			continue
		}
		newIsEqual := (&newClock).Equals(vc)
		if newIsEqual{
			fmt.Println("new is Equal")
			foundEqual = true
			newListOjectEntry = append(newListOjectEntry, objEntry)
			// don't append here
			continue
		}
	}
	if ((countCurrDesc > 0) && (countCurrDesc == len((*s).mapDataTuple[key]))){
		//dont append
	}else if foundEqual{
		// don't append
		// newListOjectEntry = append(newListOjectEntry, newObjectEntry)
	}else{
		newListOjectEntry = append(newListOjectEntry, newObjectEntry)
		// fmt.Println("appending to newListObjectEntry")
	}

	//todo: check what failures conditions are here.
	fmt.Println("PutLocal, node=", (*s).nodeID, " key=", key, "stored(put)=", newListOjectEntry)
	// newObjectEntry := ObjectEntry{Context: value.Context,
	// 		Value: value.Value,}
	// currListObjectEntry := (*s).mapDataTuple[key] //??? check assignments here
	// currListObjectEntry = append(currListObjectEntry, newObjectEntry)
	
	(*s).mapDataTuple[key] = newListOjectEntry
	
	(*result) = true
	// currIsDesc := newClock.LessThan(existingClock)
	// if currIsDesc{
	// 	//fail here
	// 	(*result) = false // ??? should we set false here if value is not updated?
	// 	//??? should we set to w other nodes here as well?  
	// 	return errors.New("Put failed")
	// }
	// //1.1 if new is descendant
	// newIsDesc := existingClock.LessThan(newClock)
	// if newIsDesc{
	// 	(*result) = true
	// 	//replace the old one
	// 	newContext := value.Context
	// 	newContext.Clock.PairClock[(*s).nodeID] = newContext.Clock.PairClock[(*s).nodeID]+1 
	// 	newObjectEntry := ObjectEntry{Context: newContext,
	// 		Value: value.Value,}
	// 	(*s).mapDataTuple[key] = []ObjectEntry{newObjectEntry}
	// 	// return nil
	// 	// todo: complete this
	// }

	// //1.2. If concurrent, store both the values? 
	// newIsConcurrent := existingClock.Concurrent(newClock)
	// if newIsConcurrent{
	// 	newObjectEntry := ObjectEntry{Context: value.Context,
	// 									Value: value.Value,}
	// 	currListObjectEntry := (*s).mapDataTuple[key] //??? check assignments here
	// 	currListObjectEntry = append(currListObjectEntry, newObjectEntry)
	// 	(*s).mapDataTuple[key] = currListObjectEntry
	// 	// return nil
	// }
	//replication
	//2. todo: replication
	//3. Increment the vector clock 
	//??? should increment here ? if 
	//4. todo: Store nodes for future gossip operation 
	// panic("todo")
	//***** replication using put

	//***** 
	return nil
}
func (s *DynamoServer) Get(key string, result *DynamoResult) error {
	fmt.Println("......Staring GET.....for key", key)
	if (*s).crashState{ //todo: should get fail when empty list is returned
		return errors.New("Node crashed")		
	}
	// fmt.Println("Get called from server side for key", key)
	//1. implementing for self node as of now
	//2. todo: check if entry list is returned dynamo result
	//*** REMOVE this *****
	requiredVotes := (*s).rValue
	counter := 0
	succCnt := 0
	var mergedObjEntry []ObjectEntry
	//self is taken first 
	// mutex.Lock()
	theDataTupleList, ok := (*s).mapDataTuple[key]
	succCnt += 1 //as self is not crashed here.
	if ok{
		mergedObjEntry = theDataTupleList
		// fmt.Println("mergedObjEntry from Local", mergedObjEntry)
		// put this in set of desc and concurrent
	}
	// fmt.Println("preference List", (*s).preferenceList)
	for ;(succCnt<requiredVotes) && (counter<(len((*s).preferenceList))); counter++{
		//skip self here
		// fmt.Println("In for loop for counter")
		dNode := (*s).preferenceList[counter]
		if (*s).SelfCheck(dNode){ //skip if self node in preference list
			// fmt.Println("found self in Get Preference List")
			continue
		}
		serverAddr := dNode.Address+":" + dNode.Port
		serverClientInstance := NewDynamoServerClient(serverAddr)
		serverClientInstance.ConnDynamoServer()
		var responseFromGet DynamoResult //todo: check if this is nil
		succ := serverClientInstance.GetSingleWrapper(key, &responseFromGet)//should return succ on when not crashed
		//if not crashed
		if succ{
			succCnt += 1
			mergedObjEntry = mergeDynamoResult(mergedObjEntry, responseFromGet.EntryList)
		}else{
			continue
		}
	}
	(*result) = DynamoResult{EntryList: mergedObjEntry}
	// mutex.Unlock()
	return nil //todo: should this crash if empty entrylist
}

func (dynamoServerClient *DynamoServerClient) GetSingleWrapper(key string, result *DynamoResult)bool{ //rpc call
	//connect and call get
	fmt.Println("......GetSingleWrapper Called.....")
	if dynamoServerClient.rpcConn == nil {
		return false //todo: check if this should not be counted, if can't be contacted
	}
	//todo: check this again Put Local 
	err := dynamoServerClient.rpcConn.Call("MyDynamo.GetSingle", key, &result)
	if err != nil {
		log.Println(err)
		return false
	}
	return true
}

func (s *DynamoServer) GetSingle(key string, result *DynamoResult)error{ //rpc call
	if (*s).crashState{
		return errors.New("Node crashed")		
	}
	theDataTupleList, ok := (*s).mapDataTuple[key]
	if ok{
		(*result) =  DynamoResult{EntryList: theDataTupleList}
	}else{
		(*result) = DynamoResult{EntryList: []ObjectEntry{}}
	}
	// fmt.Println(".....Result from GET Single", *result)
	return nil
	//connect and call get
}

func mergeDynamoResult(basicRes []ObjectEntry, mergeThis []ObjectEntry) []ObjectEntry{
	//both will be lists, and with concurrent entries
	//todo: handle empty list as well
	stageEntryList := []ObjectEntry{}
	mergedObjEntry := basicRes
	fmt.Println(".....Starting Merging for GET.....")
	fmt.Println(">>> basicResponse", basicRes)
	fmt.Println(">>> mergeThis", mergeThis)
	for _, toBeMergedEntry := range mergeThis{
		countCurrDesc := 0
		foundEqual := false
		for _, baseEntry := range mergedObjEntry{
			//append to stageEntryList
			baseClock := baseEntry.Context.Clock
			newClock := toBeMergedEntry.Context.Clock
			currIsDesc := newClock.LessThan(baseClock)
			// fmt.Println("newClock", newClock, "baseClock", baseClock)
			if currIsDesc{
				fmt.Println("current is Desc of this OBJ")
				stageEntryList = append(stageEntryList, baseEntry) //don't append here
				countCurrDesc += 1
				continue
			}
			newIsConcurrent := newClock.Concurrent(baseClock)
			if newIsConcurrent{
				fmt.Println("new is Concurrent")
				stageEntryList = append(stageEntryList, baseEntry) //append here
				continue
			}
			newIsDesc := baseClock.LessThan(newClock)
			if newIsDesc{
				fmt.Println("new is Descendant")
				//don't add vc to new list. will have to append newClock
				continue
			}
			newIsEqual := (&newClock).Equals(baseClock)
			if newIsEqual{
				fmt.Println("new is Equal")
				foundEqual = true
				stageEntryList = append(stageEntryList, baseEntry)
				// don't append here
				continue
			}
			//equal
			//concurrent
			//baseEntry Desc
			//toBeMerged Desc
		}
		if ((countCurrDesc > 0) && (countCurrDesc == len(mergedObjEntry))){
			//dont append
		}else if foundEqual{
			// append
			// stageEntryList = append(stageEntryList, toBeMergedEntry)
		}else{
			stageEntryList = append(stageEntryList, toBeMergedEntry)
			// fmt.Println("Appended to stageEntryList")
		}
		mergedObjEntry = stageEntryList
		stageEntryList = []ObjectEntry{}
	}
	fmt.Println(">>final mergedObjEntry from all", mergedObjEntry)
	fmt.Println(".....Ending Merging GET.....")
	return mergedObjEntry
}
//Get a file from this server, matched with R other servers


/* Belows are functions that implement server boot up and initialization */
func NewDynamoServer(w int, r int, hostAddr string, hostPort string, id string) DynamoServer {
	preferenceList := make([]DynamoNode, 0)
	selfNodeInfo := DynamoNode{
		Address: hostAddr,
		Port:    hostPort,
	}
	emptyMapDataTuple := map[string][]ObjectEntry{}
	return DynamoServer{
		wValue:         w,
		rValue:         r,
		preferenceList: preferenceList,
		selfNode:       selfNodeInfo,
		nodeID:         id,
		crashState: 	false,
		// nodeVectorClock: 0,
		mapDataTuple: emptyMapDataTuple, 
	}
}

func ServeDynamoServer(dynamoServer DynamoServer) error {
	rpcServer := rpc.NewServer()
	e := rpcServer.RegisterName("MyDynamo", &dynamoServer)
	if e != nil {
		log.Println(DYNAMO_SERVER, "Server Can't start During Name Registration")
		return e
	}

	log.Println(DYNAMO_SERVER, "Successfully Registered the RPC Interfaces")

	l, e := net.Listen("tcp", dynamoServer.selfNode.Address+":"+dynamoServer.selfNode.Port)
	if e != nil {
		log.Println(DYNAMO_SERVER, "Server Can't start During Port Listening")
		return e
	}

	log.Println(DYNAMO_SERVER, "Successfully Listening to Target Port ", dynamoServer.selfNode.Address+":"+dynamoServer.selfNode.Port)
	log.Println(DYNAMO_SERVER, "Serving Server Now")

	return http.Serve(l, rpcServer)
}


//issues: 
// older version is given and then we increment the version and now it's equal , what to do?