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
	inputFiles []string
	nReduce    int
	accessLock sync.Mutex
	allFiles   []string

	// Map Stage
	toStartMaps    []int
	inProgressMaps map[int]bool
	completedMaps  map[int]bool

	// Reduce Stage
	toStartReduces    []int
	inProgressReduces map[int]bool
	completedReduces  map[int]bool

	isMapCompleted bool
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) ServeRequest(args *RequestWorkArgs, reply *RequestWorkReply) error {
	m.accessLock.Lock()
	defer m.accessLock.Unlock()

	if !m.isMapCompleted && len(m.toStartMaps) > 0 {
		return m.ServeMap(args, reply)
	} else if m.isMapCompleted && len(m.toStartReduces) > 0 {
		return m.ServeReduce(args, reply)
	}
	return nil
}

func (m *Master) ServeCompletion(args *CompleteWorkArgs, reply *CompleteWorkArgs) error {
	m.accessLock.Lock()
	defer m.accessLock.Unlock()

	if args.CompletionType == "map" {
		m.handleMapCompletion(args)
		if len(m.completedMaps) == len(m.inputFiles) {
			log.Printf("Map stage completed!")
			m.toStartReduces = makeRange(0, m.nReduce-1)
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
	if _, ok := m.inProgressMaps[args.MapId]; ok {
		delete(m.inProgressMaps, args.MapId)
		m.completedMaps[args.MapId] = true
	}
}

func (m *Master) handleReduceCompletion(args *CompleteWorkArgs) {
	if _, ok := m.inProgressReduces[args.ReduceId]; ok {
		delete(m.inProgressReduces, args.ReduceId)
		m.completedReduces[args.ReduceId] = true
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
	mapId, remainingMapId := m.toStartMaps[0], m.toStartMaps[1:]

	m.toStartMaps = remainingMapId
	m.inProgressMaps[mapId] = true

	reply.WorkType = "map"
	reply.FileName = m.allFiles[mapId]
	reply.MapId = mapId
	reply.ReduceTotal = m.nReduce
	time.AfterFunc(10*time.Second, func() {
		m.purgeMapAfterTimeout(mapId)
	})

	return nil
}

func (m *Master) ServeReduce(args *RequestWorkArgs, reply *RequestWorkReply) error {
	var reduceId int
	var remainingReduceIds []int
	reduceId, remainingReduceIds = m.toStartReduces[0], m.toStartReduces[1:]
	m.toStartReduces = remainingReduceIds
	m.inProgressReduces[reduceId] = true

	reply.WorkType = "reduce"
	reply.ReduceId = reduceId
	time.AfterFunc(10*time.Second, func() {
		m.purgeReduceAfterTimeout(reduceId)
	})

	return nil
}

func (m *Master) purgeMapAfterTimeout(mapId int) {
	m.accessLock.Lock()
	m.accessLock.Unlock()

	if _, ok := m.inProgressMaps[mapId]; ok {
		delete(m.inProgressMaps, mapId)
		m.toStartMaps = append(m.toStartMaps, mapId)
	}
}

func (m *Master) purgeReduceAfterTimeout(reduceId int) {
	m.accessLock.Lock()
	m.accessLock.Unlock()

	if _, ok := m.inProgressReduces[reduceId]; ok {
		delete(m.inProgressReduces, reduceId)
		m.toStartReduces = append(m.toStartReduces, reduceId)
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
	ret := len(m.completedReduces) == m.nReduce
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		inputFiles: files,
		nReduce:    nReduce,
		allFiles:   files,
		isMapCompleted: false,

		toStartMaps:    makeRange(0, len(files)-1),
		inProgressMaps: make(map[int]bool),
		completedMaps:  make(map[int]bool),

		inProgressReduces: make(map[int]bool),
		completedReduces:  make(map[int]bool),
	}

	// Your code here.

	m.server()
	return &m
}
