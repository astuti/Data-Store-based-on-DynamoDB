package mydynamo
import (
	// "fmt"
    "reflect"
)
type VectorClock struct {
	//todo
	PairClock map[string]int // version is int here
}

//Creates a new VectorClock
func NewVectorClock() VectorClock {
	//assuming 0 for hosts here
	emptyMap := map[string]int{}
	return VectorClock{PairClock: emptyMap} 
	// panic("todo")
}

//Returns true if the other VectorClock is causally descended from this one
func (s VectorClock) LessThan(otherClock VectorClock) bool {
	// fmt.Print("LLLLLLLLLess than new")
	verGreaterThanEqual := true
	equal := true

	var keys1 []string
	var keys2 []string

	for k, _ := range s.PairClock{
		keys1 = append(keys1, k)
	}
	
	for k, _ := range otherClock.PairClock{
		keys2 = append(keys2, k)
	}

	for _, k := range keys1{
		element, oks := s.PairClock[k]
		if !oks{
			element = 0
		}
		otherElement, oko := otherClock.PairClock[k]
		if !oko{
			otherElement = 0
		}
		if otherElement > element{
			equal = false
		}else if(otherElement == element){
		}else if(otherElement < element){
			verGreaterThanEqual = false
			equal = false
		}
	}

	for _, k := range keys2{
		element, oks := s.PairClock[k]
		if !oks{
			element = 0
		}
		otherElement, oko := otherClock.PairClock[k]
		if !oko{
			otherElement = 0
		}
		if otherElement > element{
			equal = false
		}else if(otherElement == element){
		}else if(otherElement < element){
			verGreaterThanEqual = false
			equal = false
		}
	}
	
	// for key, element := range s.PairClock{
	// 	// fmt.Println("Key:", key, "=>", "Element:", element)
	// 	otherElement, ok := otherClock.PairClock[key]
	// 	if !ok{
	// 		otherElement = 0
	// 	}
	// 	if otherElement > element{
	// 		equal = false
	// 	}else if(otherElement == element){
	// 	}else if(otherElement < element){
	// 		verGreaterThanEqual = false
	// 		equal = false
	// 	}
	// }
	return verGreaterThanEqual && !equal
	// panic("todo")
}

//Returns true if neither VectorClock is concurrent
func (s VectorClock) Concurrent(otherClock VectorClock) bool {
	//**returns false if equal
	if (&s).Equals(otherClock){
		return false
	} 
	//concurrent if no one is less than the other
	otherIsLess := s.LessThan(otherClock)
	thisIsLess := otherClock.LessThan(s)
	if ((!otherIsLess) && (!thisIsLess)){
		return true
	}
	return false
	// panic("todo")
}

//Increments this VectorClock at the element associated with nodeId
func (s *VectorClock) Increment(nodeId string) {
	currVersion, ok := (*s).PairClock[nodeId]
	if !ok{
		currVersion = 0
	}
	(*s).PairClock[nodeId] = currVersion+1
	// panic("todo")
}

//Changes this VectorClock to be causally descended from all VectorClocks in clocks
func (s *VectorClock) Combine(clocks []VectorClock) {
	// var allNodeIds []string
	//also accounts for s in calculating descendants
	for _, vc := range clocks{
		for nodeId, ver := range vc.PairClock{
			thisVer, ok := (*s).PairClock[nodeId]
			if !ok{
				thisVer = 0
			}
			(*s).PairClock[nodeId] = Max(thisVer, ver)
		}
	}
	// for key, element := range (*s).PairClock{
		
	// }
	// panic("todo")
}

//Tests if two VectorClocks are equal
func (s *VectorClock) Equals(otherClock VectorClock) bool {
	pairClockThis := (*s).PairClock
	pairClockOther := otherClock.PairClock
	cmp := reflect.DeepEqual(pairClockThis, pairClockOther)
	return cmp
	// panic("todo")
}

func Max(x, y int) int {
	if x > y {
	  return x
	}
	return y
   }

func CreateClock(pairClockArg map[string]int ) VectorClock{
	vc := VectorClock{PairClock: pairClockArg}
	return vc
	// panic("todo")
}
