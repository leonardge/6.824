package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"sort"
	"strings"
)
import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	createDirIfNotExist("temp_map")
	for {
		// println("entering the loop!")

		request := RequestWorkArgs{
			RequestType: "map",
		}

		reply := RequestWorkReply{}

		ok := call("Master.ServeRequest", &request, &reply)
		// rpc has failed, master must already has exitted.
		if !ok {
			log.Printf("Cannot call master")
			return
		}
		if reply.FileName != "" && reply.WorkType == "map" {
			kvs := MapFile(mapf, reply)
			err := partitionAndWriteFiles(reply.ReduceTotal, kvs, reply.MapId)
			if err != nil {
				return
			}
			completeMap(reply.FileName)
			continue
		}

		if reply.WorkType == "reduce" {
			kvs := readPartition(reply.ReduceId)
			kvs = ReduceFiles(reducef, kvs)
			writeResult(fmt.Sprintf("mr-out-%d", reply.ReduceId), kvs)
			completeReduce(reply.ReduceId)
		}

	}

}

func readPartition(reduceId int) []KeyValue {
	files, err := ioutil.ReadDir("./temp_map")
	if err != nil {
		log.Fatalf("Error reading directory: %v", err)
	}

	kva := make([]KeyValue, 0)
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), fmt.Sprintf("-%d", reduceId)) {
			continue
		}

		f, err := os.Open(fmt.Sprintf("./temp_map/%s", file.Name()))
		if err != nil {
			log.Fatalf("Error opening file %s: %v", file.Name(), err)
		}

		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	return kva
}

func ReduceFiles(
	reducef func(string, []string) string, kvs []KeyValue) []KeyValue {
	sort.Sort(ByKey(kvs))
	res := make([]KeyValue, 0)
	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		output := reducef(kvs[i].Key, values)

		// this is the correct format for each line of Reduce output.
		res = append(res, KeyValue{Key:kvs[i].Key, Value: output})

		i = j
	}

	return res
}

func isStringMatching(pattern string, str string) bool{
	return regexp.MustCompile(pattern).MatchString(str)
}

func completeMap(fileName string) bool {
	request := CompleteWorkArgs{CompletionType: "map", MapFileName: fileName}
	reply := CompleteWorkReply{}
	ok := call("Master.ServeCompletion", &request, &reply)
	return ok
}

func completeReduce(reduceId int) bool {
	request := CompleteWorkArgs{CompletionType: "reduce", ReduceId: reduceId}
	reply := CompleteWorkReply{}
	ok := call("Master.ServeCompletion", &request, &reply)
	return ok
}

func partitionAndWriteFiles(nReduce int, kvs []KeyValue, mapId int) error {
	pkvs := make([][]KeyValue, nReduce)

	for i := 0; i < nReduce; i++ {
		pkvs[i] = make([]KeyValue, 0)
	}

	for _, kv := range kvs {
		pkvs[ihash(kv.Key) % nReduce] = append(pkvs[ihash(kv.Key) % nReduce], kv)
	}

	for reduceId := 0; reduceId < nReduce; reduceId++ {
		writeKVs(fmt.Sprintf("map-%d-%d", mapId, reduceId), pkvs[reduceId])
	}

	return nil
}

func MapFile(mapf func(string, string) []KeyValue, reply RequestWorkReply) []KeyValue {
	file, err := os.Open(reply.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", reply.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.FileName)
	}
	err = file.Close()
	if err != nil {
		log.Fatal("Error closing input file")
	}
	kvs := mapf(reply.FileName, string(content))
	return kvs
}

func createDirIfNotExist(path string) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		os.Mkdir(path, 0777)
	}
}

func writeKVs(fileName string, keyvalues []KeyValue) {
	file, err := ioutil.TempFile("", "map")
	if err != nil {
		return
	}
	enc := json.NewEncoder(file)
	for _, kv := range keyvalues {
		err := enc.Encode(kv)
		if err != nil {
			return
		}
	}
	err = os.Rename(file.Name(), fmt.Sprintf("./temp_map/%s",fileName))
	if err != nil {
		log.Fatalf("error renaming %v", err)
	}
	err = file.Close()
	if err != nil {
		log.Fatalf("error closing map file: %v", err)
	}
}

func writeResult(fileName string, keyvalues []KeyValue) {
	file, err := ioutil.TempFile("", "map")
	if err != nil {
		return
	}
	for _, kv := range keyvalues {
		fmt.Fprintf(file, "%v %v\n", kv.Key, kv.Value)
	}
	err = os.Rename(file.Name(), fmt.Sprintf("./temp_map/%s",fileName))
	if err != nil {
		log.Fatalf("error renaming %v", err)
	}
	err = file.Close()
	if err != nil {
		log.Fatalf("error closing map file: %v", err)
	}
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing :", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
