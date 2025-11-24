package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		args := WorkerArgs{}
		reply := TaskReply{}

		// send the RPC request, wait for the reply.
		ok := call("Coordinator.AssignTask", &args, &reply)
		if ok {
			// Handle the specific task type
			switch reply.Type {
			case Map:
				args.TaskID = reply.TaskID
				args.Type = Map
				doMap(mapf, &args, &reply)
			case Reduce:
				args.TaskID = reply.TaskID
				args.Type = Reduce
				doReduce(reducef, &args, &reply)
			case Wait:
				time.Sleep(1 * time.Second)
			case Exit:
				return
			}
		} else {
			fmt.Printf("call failed!\n")
			return
		}
	}
}

func doMap(mapf func(string, string) []KeyValue, args *WorkerArgs, reply *TaskReply) {
	/*
		1. Read the input file
		2. Call the mapf fuction
		3. Create buffers for the nReduce buckets
		4. Write to the intermediate files
		5. Report to the coordinator
	*/
	// 1. Read the input file
	filename := reply.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	// 2. Call the mapf fuction
	kva := mapf(filename, string(content))

	// 3. Create nReduce buckets
	buckets := make([][]KeyValue, reply.NReduce)
	for _, kv := range kva {
		bucketID := ihash(kv.Key) % reply.NReduce
		buckets[bucketID] = append(buckets[bucketID], kv)
	}

	// 4. Write to the intermediate files
	for i := 0; i < reply.NReduce; i++ {
		interName := fmt.Sprintf("mr-%d-%d", reply.TaskID, i)
		tempFile, err := ioutil.TempFile(".", interName+"temp")
		if err != nil {
			log.Fatalf("cannot create temp file")
		}

		enc := json.NewEncoder(tempFile)
		for _, kv := range buckets[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("Json Encoder failed")
			}
		}

		tempFile.Close()
		os.Rename(tempFile.Name(), interName)
	}

	// 5. Report to the coordinator
	ok := call("Coordinator.TaskDone", args, reply)
	if ok {
		fmt.Printf("Map Task %d completed\n", args.TaskID)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func doReduce(reducef func(string, []string) string, args *WorkerArgs, reply *TaskReply) {
	/*
		1. Read all intermediate files (eg: mr-0-Y, mr-1-Y ...)
		2. Sort by the key
		3. Call Reduce on distinct keys
		4. Report to the coordinator
	*/

	intermediate := []KeyValue{}

	// 1. Read all intermediate files (eg: mr-0-Y, mr-1-Y ...)
	for i := 0; i < reply.NMap; i++ {
		interName := fmt.Sprintf("mr-%d-%d", i, reply.TaskID)
		file, err := os.Open(interName)
		if err != nil {
			// If file doesn't exist, skip it (maybe that map produced no keys)
			continue
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	// 2. Sort by the key and create the out file
	sort.Sort(ByKey(intermediate))
	oname := fmt.Sprintf("mr-out-%d", reply.TaskID)
	tempFile, err := ioutil.TempFile(".", oname+"temp")
	if err != nil {
		log.Fatalf("cannot create temp file")
	}

	// 3. Call Reduce on distinct keys
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	tempFile.Close()
	err = os.Rename(tempFile.Name(), oname)
	if err != nil {
		log.Fatalf("cannot rename output file")
	}

	// 4. Report to the coordinator
	ok := call("Coordinator.TaskDone", args, reply)
	if ok {
		fmt.Printf("Reduce Task %d completed\n", args.TaskID)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
