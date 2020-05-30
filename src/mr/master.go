package mr

import (
	"fmt"
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"


type Master struct {
	// Your definitions here.
	inputFiles      []string
	nReduce         int
	accessLock      sync.Mutex
	toStartFiles    []string
	inProgressFiles []string
	// index for the next map task just in case it fails
	nextMapTaskIdx int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) ServeRequest(args *RequestWorkArgs, reply *RequestWorkReply) error {
	if args.RequestType == "map" {
		var file string
		var remainingFiles []string

		m.accessLock.Lock()
		if len(m.toStartFiles) == 0 {
			log.Printf("All map has been dispatched")
			return nil
		}

		file, remainingFiles = m.toStartFiles[0], m.toStartFiles[1:]
		mapId := m.nextMapTaskIdx
		m.nextMapTaskIdx += 1
		m.toStartFiles = remainingFiles
		m.accessLock.Unlock()

		reply.WorkType = "map"
		reply.FileName = file
		reply.MapId = mapId
		reply.ReduceTotal = m.nReduce
		fmt.Printf("reply: %v", reply)

		return nil
	} else {
		return fmt.Errorf("can't handle task type other than map")
	}
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	if len(m.toStartFiles) == 0 {
		print("done!\n")
		ret = true
	}

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		inputFiles:   files,
		nReduce:      nReduce,
		toStartFiles: files,
		inProgressFiles: make([]string, 0),
		nextMapTaskIdx: 0,
	}

	// Your code here.


	m.server()
	return &m
}
