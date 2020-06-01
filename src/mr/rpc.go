package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// Add your RPC definitions here.

type RequestWorkArgs struct {
	RequestType string
}

type RequestWorkReply struct {
	WorkType    string

	// map arg
	FileName    string
	ReduceTotal int
	MapId       int

	// reduce arg
	ReduceId    int
}

type CompleteWorkArgs struct {
	// either "map" or "reduce"
	CompletionType string

	ReduceId int

	MapId int
}


type CompleteWorkReply struct {}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
