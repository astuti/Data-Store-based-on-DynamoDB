package mydynamo

//Removes an element at the specified index from a list of ObjectEntry structs
func remove(list []ObjectEntry, index int) []ObjectEntry {
	return append(list[:index], list[index+1:]...)
}

func removeGossipData(list []GossipDatum, index []int) []GossipDatum {
	var newGossipData []GossipDatum

	for idx, gpDatum := range list{
		if contains(index, idx){
			//do nothing, ignore
		}else{
		newGossipData = append(newGossipData, gpDatum)
		}
	}
	return newGossipData
}
//Returns true if the specified list of ints contains the specified item
func contains(list []int, item int) bool {
	for _, v := range list {
		if v == item {
			return true
		}
	}
	return false
}

//Rotates a preference list by one, so that we can give each node a unique preference list
func RotateServerList(list []DynamoNode) []DynamoNode {
	return append(list[1:], list[0])
}

//Creates a new Context with the specified Vector Clock
func NewContext(vClock VectorClock) Context {
	return Context{
		Clock: vClock,
	}
}

//Creates a new PutArgs struct with the specified members.
func NewPutArgs(key string, context Context, value []byte) PutArgs {
	return PutArgs{
		Key:     key,
		Context: context,
		Value:   value,
	}
}

//Creates a new DynamoNode struct with the specified members
func NewDynamoNode(addr string, port string) DynamoNode {
	return DynamoNode{
		Address: addr,
		Port:    port,
	}
}

func CreateVectorClock(PairClockArg map[string]int ) VectorClock{
	vc := VectorClock{PairClock: PairClockArg}
	return vc
	// panic("todo")
}