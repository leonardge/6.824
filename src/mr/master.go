package mr

import (
	"log"
	"sync"
	"time"
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
		m.handleMapCompletion(args)
		if len(m.completedFiles) == len(m.inputFiles) {
			log.Printf("Map stage completed!")
			m.toStartReduce = makeRange(0, m.nReduce-1)
			m.isMapCompleted = true
		}
	} else if args.CompletionType == "reduce" {
		m.handleReduceCompletion(args)
	} else {
		log.Fatalf("It shouldn't be here.")
	}

	return nil
}

func (m *Master) handleMapCompletion(args *CompleteWorkArgs) {
	if _, ok := m.inProgressFiles[args.MapFileName]; ok {
		delete(m.inProgressFiles, args.MapFileName)
		m.completedFiles[args.MapFileName] = true
	}
}

func (m *Master) handleReduceCompletion(args *CompleteWorkArgs) {
	if _, ok := m.inProgressReduce[args.ReduceId]; ok {
		delete(m.inProgressReduce, args.ReduceId)
		m.completedReduce[args.ReduceId] = true
	}
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
	time.AfterFunc(10 * time.Second, func() {
		m.purgeMapAfterTimeout(file)
	})

	return nil
}

func (m *Master) ServeReduce(args *RequestWorkArgs, reply *RequestWorkReply) error {
	var reduceId int
	var remainingReduceIds []int
	reduceId, remainingReduceIds = m.toStartReduce[0], m.toStartReduce[1:]
	m.toStartReduce = remainingReduceIds
	m.inProgressReduce[reduceId] = true

	reply.WorkType = "reduce"
	reply.ReduceId = reduceId
	time.AfterFunc(10 * time.Second, func() {
		m.purgeReduceAfterTimeout(reduceId)
	})

	return nil
}

func (m *Master) purgeMapAfterTimeout(fileName string) {
	m.accessLock.Lock()
	m.accessLock.Unlock()

	if _, ok := m.inProgressFiles[fileName]; ok {
		delete(m.inProgressFiles, fileName)
		m.toStartFiles = append(m.toStartFiles, fileName)
	}
}

func (m *Master) purgeReduceAfterTimeout(reduceId int) {
	m.accessLock.Lock()
	m.accessLock.Unlock()

	if _, ok := m.inProgressReduce[reduceId]; ok {
		delete(m.inProgressReduce, reduceId)
		m.toStartReduce = append(m.toStartReduce, reduceId)
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

		toStartFiles:   files,
		inProgressFiles:  make(map[string]bool),
		completedFiles: make(map[string]bool),

		inProgressReduce: make(map[int]bool),
		completedReduce:  make(map[int]bool),

		nextMapTaskIdx: 0,
		isMapCompleted: false,

	}

	// Your code here.


	m.server()
	return &m
}
