package mr

import (
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"


type Master struct {
	// Your definitions here.
	inputFiles       []string
	nReduce          int
	accessLock       sync.Mutex

	// Map Stage
	toStartFiles     []string
	inProgressFiles  map[string]bool
	completedFiles   map[string]bool

	// Reduce Stage
	toStartReduce    []int
	inProgressReduce map[int]bool
	completedReduce  map[int]bool

	// index for the next map task just in case it fails
	nextMapTaskIdx int
	isMapCompleted bool
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
	m.accessLock.Lock()
	defer m.accessLock.Unlock()

	if !m.isMapCompleted && len(m.toStartFiles) > 0 {
		return m.ServeMap(args, reply)
	} else if m.isMapCompleted && len(m.toStartReduce) > 0 {
		return m.ServeReduce(args, reply)
	}
	return nil
}

func (m *Master) ServeCompletion(args *CompleteWorkArgs, reply *CompleteWorkArgs) error {
	m.accessLock.Lock()
	defer m.accessLock.Unlock()

	if args.CompletionType == "map" {
		delete(m.inProgressFiles, args.MapFileName)
		m.completedFiles[args.MapFileName] = true
		if len(m.completedFiles) == len(m.inputFiles) {
			log.Printf("Map stage completed!")
			m.toStartReduce = makeRange(0, m.nReduce - 1)
			m.isMapCompleted = true
		}
	} else if args.CompletionType == "reduce" {
		delete(m.inProgressReduce, args.reduceId)
		m.completedReduce[args.reduceId] = true
	} else {
		log.Fatalf("It shouldn't be here.")
	}

	return nil
}

func makeRange(min, max int) []int {
	a := make([]int, max-min+1)
	for i := range a {
		a[i] = min + i
	}
	return a
}

func (m *Master) ServeMap(args *RequestWorkArgs, reply *RequestWorkReply) error {
	var file string
	var remainingFiles []string
	file, remainingFiles = m.toStartFiles[0], m.toStartFiles[1:]
	mapId := m.nextMapTaskIdx
	m.nextMapTaskIdx += 1
	m.toStartFiles = remainingFiles
	m.inProgressFiles[file] = true

	reply.WorkType = "map"
	reply.FileName = file
	reply.MapId = mapId
	reply.ReduceTotal = m.nReduce

	return nil
}

func (m *Master) ServeReduce(args *RequestWorkArgs, reply *RequestWorkReply) error {
	reply.WorkType = "reduce"
	return nil
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
	m.accessLock.Lock()
	defer m.accessLock.Unlock()
	ret := len(m.completedReduce) == m.nReduce
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
		inProgressFiles: make(map[string]bool),
		completedFiles: make(map[string]bool),
		nextMapTaskIdx: 0,
		isMapCompleted: false,
	}

	// Your code here.


	m.server()
	return &m
}
