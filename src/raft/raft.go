package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyChan chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state
	currentTerm int
	votedFor    int // -1 signal for not voted yet.
	log         []LogEntry

	// volatile state
	commitIndex int
	lastApplied int

	// volatile state on leader
	nextIndex  []int
	matchIndex []int

	// additional state
	role                          int // 0 means leader, 1 means candidate and 2 means follower
	voteCount                     int // the number of votes received as candidate
	lastAppendEntriesReceivedTime int64

	// TODO: it is probably also a good idea to store the config, e.g. timeout in here
}

type LogEntry struct {
	Command interface{}
	Term    int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.role == 0
	rf.mu.Unlock()

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//log.Printf("request vote: %s, args: %v", rf.getStateString(), args)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = 2
		rf.votedFor = -1
		rf.voteCount = 0
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		  rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		return
	}

}

func (rf *Raft) isLogUpToDate(lastLogIndex int, lastLogTerm int) bool {
	return lastLogIndex >= rf.getLastLogIndex() && lastLogTerm >= rf.getLastLogTerm()
}

//
// example AppendEntriesReply RPC arguments structure.
// field names must start with capital letters!
//
type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

//
// example AppendEntriesReply RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	//fmt.Printf("args: %+v, state: %s \n", args, rf.getStateString())
	if args.PrevLogIndex != 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term >= rf.currentTerm {
		rf.lastAppendEntriesReceivedTime = time.Now().UnixNano()
		rf.votedFor = -1


		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.role = 2
			rf.votedFor = -1
			rf.voteCount = 0
		}

		if args.LeaderCommit > rf.commitIndex {
			newCommitIdx := min(args.LeaderCommit, rf.getLastLogIndex() + len(args.Entries))
			if newCommitIdx > rf.commitIndex {
				rf.commitIndex = newCommitIdx
				for idx := newCommitIdx; idx < newCommitIdx + 1; idx++ {
					rf.applyChan <- ApplyMsg{
						CommandValid: true,
						Command:      rf.log[idx].Command,
						CommandIndex: idx,
					}
				}
				fmt.Printf(">>>update cidx: %d, lcidx: %d r: %d, log: %v\n",
					rf.commitIndex, args.LeaderCommit, rf.getLastLogIndex() + len(args.Entries), rf.log)

			}
		}

		//fmt.Printf("append entry, args: %+v\n", args)
		fmt.Printf(">>>log: %v\n", rf.log)
		// Add one to avoid remove the dummy log entry.
		rf.log = append(rf.log[:args.PrevLogIndex + 1], args.Entries...)
		reply.Success = true
		reply.Term = rf.currentTerm
	}

}

func min(num1 int, num2 int) int {
	if num1 < num2 {
		return num1
	}
	return num2
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	if rf.killed() {
		return -1, -1, false
	}

	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index = rf.getLastLogIndex() + 1
	term = rf.currentTerm
	isLeader = rf.role == 0
	if !isLeader {
		return -1, -1, false
	}

	fmt.Printf("start: %v\n", command)
	rf.log = append(rf.log, LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	})

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyChan = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = initializeLog()

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = initializeNextIndex(len(peers))
	rf.matchIndex = initializeMatchIndex(len(peers))

	rf.role = 2
	rf.voteCount = 0
	rf.lastAppendEntriesReceivedTime = time.Now().UnixNano()


	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.StartAppendEntriesLoop()

	go rf.StartRequestVoteLoop()

	return rf
}

func initializeLog() []LogEntry {
	dummyLog := LogEntry{}
	return []LogEntry{dummyLog}
}

func initializeMatchIndex(size int) []int {
	return make([]int, size)
}

func initializeNextIndex(size int) []int {
	nextIndex := make([]int, size)
	for idx, _ := range nextIndex {
		nextIndex[idx] = 1
	}
	return nextIndex
}

func (rf *Raft) StartAppendEntriesLoop() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.role == 0 {
			rf.broadcastAppendEntries()
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * 150)
	}
}

func (rf *Raft) broadcastAppendEntries() {
	var entries []LogEntry
	for peerIdx := range rf.peers {
		if peerIdx == rf.me {
			continue
		}

		entries = make([]LogEntry, 0)
		prevLogIndex := rf.getLastLogIndex()
		//fmt.Printf("last log index: %d nextindex: %v\n", rf.getLastLogIndex(), rf.nextIndex)
		if rf.getLastLogIndex() >= rf.nextIndex[peerIdx] {
			entries = append(entries, rf.log[rf.nextIndex[peerIdx]:]...)
			prevLogIndex = prevLogIndex - len(entries)
		}

		// Passed all the parameters in to avoid the parameters change when the go routine is executed.
		go rf.sendAndProcessAppendEntriesRPC(rf.currentTerm, rf.me, prevLogIndex, rf.log[prevLogIndex].Term, entries, rf.commitIndex, peerIdx)
	}
}

func (rf *Raft) sendAndProcessAppendEntriesRPC(
		term int, leaderId int, prevLogIndex int, prevLogTerm int,
		entries []LogEntry, leaderCommit int, receiverId int) {
	appendEntriesArgs := AppendEntriesArgs{
		Term:         term,
		LeaderId:     leaderId,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}

	//fmt.Printf("r: %d\narg: %+v\nstate: %+v\n",receiverId, appendEntriesArgs, rf.getStateString())

	appendEntriesReply := AppendEntriesReply{}
	//fmt.Printf("full log: %+v\n", rf.log)
	rf.sendAppendEntries(receiverId, &appendEntriesArgs, &appendEntriesReply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// The only case when term can be 0 is when it timeout. So we should ignore this reply.
	if appendEntriesReply.Term == 0 {
		return
	}

	// Not leader anymore, do nothing.
	if rf.role != 0 {
		return
	}

	if appendEntriesReply.Term > rf.currentTerm {
		rf.currentTerm = appendEntriesReply.Term
		rf.role = 2
		rf.votedFor = -1
		rf.voteCount = 0
		return
	}

	if appendEntriesReply.Success {
		newCommitIndex := rf.commitIndex + 1

		rf.nextIndex[receiverId] += len(entries)
		rf.matchIndex[receiverId] += len(entries)

		cnt := 0
		for _, idx := range rf.matchIndex {
			if idx >= newCommitIndex {
				cnt += 1
			}
		}

		fmt.Printf("To update cindex: %s, cindex: %d r: %d, e: %d \n",
			rf.getStateString(), rf.commitIndex, receiverId, len(entries))
		if cnt >= (len(rf.peers) / 2 + 1) && rf.log[newCommitIndex].Term == rf.currentTerm {
			rf.commitIndex = newCommitIndex
			rf.applyChan <- ApplyMsg{
				CommandValid: true,
				Command:      rf.log[newCommitIndex].Command,
				CommandIndex: newCommitIndex,
			}
			fmt.Printf("Updated commit index\n")
		}

	} else {
		fmt.Printf("appendEntry args: %+v, reply: %+v", appendEntriesArgs, appendEntriesReply)
		fmt.Printf(" state: %s, receiver id: %d \n", rf.getStateString(), receiverId)
		rf.nextIndex[receiverId] -= 1
	}
}

func (rf *Raft) StartRequestVoteLoop() {
	for !rf.killed() {
		rf.mu.Lock()

		// Follower Timeout and start election
		if rf.role == 2 && rf.votedFor == -1 && rf.exceedElectionTimeout() {
			// fmt.Printf("Follower becomes candidate: %s\n", rf.getStateString())
			rf.StartElection()
		} else if rf.role == 1 && rf.exceedElectionTimeout() { // Candidate election timeout
			// fmt.Printf("Candidate election timeout: %s\n", rf.getStateString())
			rf.StartElection()
		}

		rf.mu.Unlock()
		time.Sleep(time.Millisecond * 100)
	}
}

func (rf *Raft) StartElection() {

	rf.currentTerm += 1

	rf.votedFor = rf.me
	rf.voteCount = 1

	rf.lastAppendEntriesReceivedTime = time.Now().UnixNano()

	rf.role = 1

	//log.Printf("start election: %s", rf.getStateString())
	for peerIdx := range rf.peers {
		if peerIdx == rf.me {
			continue
		}

		// Passed all the parameters in is to avoid the parameters change when the go routine is executed.
		go rf.sendAndProcessRequestVoteRPC(rf.currentTerm, rf.me, len(rf.log), 0, peerIdx)
	}
}

func (rf *Raft) sendAndProcessRequestVoteRPC(term int, candidateId int, lastLogIndex int, lastLogTerm int, receiverIdx int) {
	requestVoteArgs := RequestVoteArgs{
		Term:         term,
		CandidateId:  candidateId,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	requestVoteReply := RequestVoteReply{}
	rf.sendRequestVote(receiverIdx, &requestVoteArgs, &requestVoteReply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// handle the reply...
	if requestVoteReply.Term > rf.currentTerm {
		rf.currentTerm = requestVoteReply.Term
		rf.role = 2
		rf.votedFor = -1
		rf.voteCount = 0
		return
	}

	if requestVoteReply.VoteGranted && requestVoteReply.Term == rf.currentTerm {
		rf.voteCount += 1
		//log.Printf("Collected one vote: %s", rf.getStateString())
		if rf.role == 1 && rf.voteCount >= (len(rf.peers)/2+1) {
			log.Printf("Becomes leader: %s", rf.getStateString())
			rf.role = 0
			rf.broadcastAppendEntries()
		}
		return
	}

}

func (rf *Raft) exceedElectionTimeout() bool {
	elapsed := time.Since(time.Unix(0, rf.lastAppendEntriesReceivedTime))
	return elapsed > (time.Millisecond * time.Duration(400+rand.Intn(400)))
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) getLastLogTerm() int {
	return rf.log[rf.getLastLogIndex()].Term
}

// This needs to be called while mutex is held.
func (rf *Raft) getStateString() string {
	return fmt.Sprintf("term: %d, id: %d, role: %d", rf.currentTerm, rf.me, rf.role)
}
