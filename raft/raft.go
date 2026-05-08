package raft

//
// This is an outline of the API that raft must expose to
// the service (or tester). See comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   Create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   Start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   Each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester) in the same server.
//

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"cs351/labrpc"
)

// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). Set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type State int

// added state constants for easier readability
const (
	Follower  State = 0
	Candidate State = 1
	Leader    State = 2
)

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu    sync.Mutex          // Lock to protect shared access to this peer's state
	peers []*labrpc.ClientEnd // RPC end points of all peers
	me    int                 // This peer's index into peers[]
	dead  int32               // Set by Kill()

	// Your data here (3A, 3B).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	logs        []LogEntry // index, term, command
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	state            State
	electionDeadline time.Time
	lastHeartbeat    time.Time // for the leader, last time we sent heartbeats
	votes            int       // votes received in current election (candidate only)

	applyCh chan ApplyMsg // from Make(); do not send while holding mu
}

// Return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (3A).

	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	rf.mu.Unlock()

	return term, isleader
}

// Example RequestVote RPC arguments structure.
// Field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term        int
	CandidateId int
	// these two are used to check the election restriction
	LastLogIndex int
	LastLogTerm  int
}

// Example RequestVote RPC reply structure.
// Field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	// for cathing up (follower to leader) when Success is false after term check
	XLen   int // len(follower's log) if prev index not there
	XTerm  int // term at follower's prev index if termsnot the same
	XIndex int // first index in follower's log with XTerm 
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.Term = rf.currentTerm

	// reject stale term
	if args.Term < rf.currentTerm {
		return
	}
	// Higher term so step down and follow new term
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		reply.Term = rf.currentTerm
	}
	rf.state = Follower

	// 5.3: prev log entry must exist and agree on term
	if args.PrevLogIndex < 0 || args.PrevLogIndex >= len(rf.logs) {
		reply.XLen = len(rf.logs)
		return
	}
	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.XTerm = rf.logs[args.PrevLogIndex].Term
		x := args.PrevLogIndex
		for x > 0 && rf.logs[x-1].Term == reply.XTerm {
			x--
		}
		reply.XIndex = x
		return
	}

	// If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it and append any new entries not already in the log
	base := args.PrevLogIndex + 1

merge:
	for i := range args.Entries {
		idx := base + i
		if idx < len(rf.logs) {
			if rf.logs[idx].Term != args.Entries[i].Term {
				rf.logs = rf.logs[:idx]
				rf.logs = append(rf.logs, args.Entries[i:]...)
				break merge
			}
			// same index and term — already matches
		} else {
			rf.logs = append(rf.logs, args.Entries[i:]...)
			break merge
		}
	}

	// advance commit index
	if args.LeaderCommit > rf.commitIndex {
		lastIdx := len(rf.logs) - 1
		if args.LeaderCommit < lastIdx {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastIdx
		}
	}

	reply.Success = true
	reply.Term = rf.currentTerm
	rf.electionDeadline = time.Now().Add(time.Duration(rand.Intn(200)+400) * time.Millisecond)
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	//args is candidate term and rf is current term of follower
	if args.Term < rf.currentTerm {
		return
	}
	// if higher term: adopt term, forget vote, become follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		reply.Term = rf.currentTerm
	}

	// Receiver's last log index and term
	lastIdx := -1
	lastTerm := 0
	if len(rf.logs) > 0 {
		lastIdx = len(rf.logs) - 1
		lastTerm = rf.logs[lastIdx].Term
	}

	// election restriction
	logOk := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIdx)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && logOk {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.electionDeadline = time.Now().Add(time.Duration(rand.Intn(200)+400) * time.Millisecond)
	}
}

// Example code to send a RequestVote RPC to a server.
// Server is the index of the target server in rf.peers[].
// Expects RPC arguments in args. Fills in *reply with RPC reply,
// so caller should pass &reply.
//
// The types of the args and reply passed to Call() must be
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
// Look at the comments in ../labrpc/labrpc.go for more details.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// electionTimeout returns a randomized follower/candidate timeout
func (rf *Raft) electionTimeout() time.Duration {
	return time.Duration(400+rand.Intn(200)) * time.Millisecond
}

// startElection begins a new election.
func (rf *Raft) startElection() {
	rf.mu.Lock()
	if rf.state == Leader {
		rf.mu.Unlock()
		return
	}
	rf.currentTerm++
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.votes = 1
	term := rf.currentTerm
	me := rf.me
	lastIdx := len(rf.logs) - 1
	lastTerm := rf.logs[lastIdx].Term
	rf.electionDeadline = time.Now().Add(rf.electionTimeout())
	args := &RequestVoteArgs{
		Term:         term,
		CandidateId:  me,
		LastLogIndex: lastIdx,
		LastLogTerm:  lastTerm,
	}
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == me {
			continue
		}
		go rf.requestVoteWorker(i, term, args)
	}
}

func (rf *Raft) requestVoteWorker(server int, term int, args *RequestVoteArgs) {
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(server, args, &reply)

	rf.mu.Lock()
	if rf.currentTerm != term || rf.state != Candidate {
		rf.mu.Unlock()
		return
	}
	if ok {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
			rf.mu.Unlock()
			return
		}
		if reply.Term < term {
			rf.mu.Unlock()
			return
		}
		if reply.VoteGranted {
			rf.votes++
			if rf.votes > len(rf.peers)/2 {
				rf.state = Leader
				rf.lastHeartbeat = time.Now()
				n := len(rf.peers)
				rf.nextIndex = make([]int, n)
				rf.matchIndex = make([]int, n)
				for j := range rf.peers {
					rf.nextIndex[j] = len(rf.logs)
					rf.matchIndex[j] = 0
				}
				rf.matchIndex[rf.me] = len(rf.logs) - 1
				rf.mu.Unlock()
				rf.broadcastHeartbeats()
				return
			}
		}
	}
	rf.mu.Unlock()
}

// broadcastHeartbeats sends AppendEntries to each follower (empty Entries when caught up for heartbeats).
func (rf *Raft) broadcastHeartbeats() {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	term := rf.currentTerm
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.replicateToPeer(i, term)
	}
}

// maybeCommit advances commitIndex when a majority replicated index N in the current term.
func (rf *Raft) maybeCommit() {
	if rf.state != Leader {
		return
	}
	n := len(rf.peers)
	last := len(rf.logs) - 1
	for N := last; N > rf.commitIndex; N-- {
		//5.4 rule
		if rf.logs[N].Term != rf.currentTerm {
			continue
		}
		count := 1 
		for j := range rf.peers {
			if j == rf.me {
				continue
			}
			if rf.matchIndex[j] >= N {
				count++
			}
		}
		if count > n/2 {
			rf.commitIndex = N
			break
		}
	}
}

// sends AppendEntries for logs[nextIndex[peer]:].
func (rf *Raft) replicateToPeer(peer int, leaderTerm int) {
	rf.mu.Lock()
	if rf.state != Leader || rf.currentTerm != leaderTerm {
		rf.mu.Unlock()
		return
	}
	me := rf.me
	ni := rf.nextIndex[peer]
	if ni < 1 {
		ni = 1
	}
	prevIdx := ni - 1
	prevTerm := rf.logs[prevIdx].Term
	entries := append([]LogEntry(nil), rf.logs[ni:]...)
	ci := rf.commitIndex
	rf.mu.Unlock()

	args := AppendEntriesArgs{
		Term:         leaderTerm,
		LeaderId:     me,
		PrevLogIndex: prevIdx,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: ci,
	}
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(peer, &args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm != leaderTerm || rf.state != Leader {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		return
	}
	if reply.Term < leaderTerm {
		return
	}
	if reply.Success {
		rf.matchIndex[peer] = prevIdx + len(entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
	} else { //catch up logic
		if reply.XLen > 0 {
			rf.nextIndex[peer] = reply.XLen
		} else if reply.XTerm > 0 {
			last := -1
			for i := len(rf.logs) - 1; i >= 0; i-- {
				if rf.logs[i].Term == reply.XTerm {
					last = i
					break
				}
			}
			if last != -1 {
				rf.nextIndex[peer] = last + 1
			} else {
				rf.nextIndex[peer] = reply.XIndex
			}
		} else if rf.nextIndex[peer] > 1 {
			rf.nextIndex[peer]--
		}
	}
	rf.maybeCommit()
}

// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. If this
// server isn't the leader, returns false. Otherwise start the
// agreement and return immediately. There is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. Even if the Raft instance has been killed,
// this function should return gracefully.
//
// The first return value is the index that the command will appear at
// if it's ever committed. The second return value is the current
// term. The third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	if rf.killed() {
		rf.mu.Unlock()
		return -1, -1, false
	}
	if rf.state != Leader || rf.nextIndex == nil {
		rf.mu.Unlock()
		return -1, -1, false
	}
	term := rf.currentTerm
	index := len(rf.logs)
	rf.logs = append(rf.logs, LogEntry{Term: term, Command: command})
	rf.matchIndex[rf.me] = len(rf.logs) - 1
	leaderTerm := term
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.replicateToPeer(i, leaderTerm)
	}
	return index, term, true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// func (rf *Raft) sendAppendEntriesToAll(args *AppendEntriesArgs, reply *AppendEntriesReply) {
// 	for i := range rf.peers {
// 		if i == rf.me {
// 			continue
// 		}
// 		ok := rf.sendAppendEntries(i, args, reply)
// 		if !ok {
// 			return true
// 		}
// 	}
// }

// The tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. Your code can use killed() to
// check whether Kill() has been called. The use of atomic avoids the
// need for a lock.
//
// The issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. Any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		time.Sleep(15 * time.Millisecond)

		rf.mu.Lock()
		if rf.state == Leader {
			if time.Since(rf.lastHeartbeat) >= 100*time.Millisecond {
				rf.lastHeartbeat = time.Now()
				rf.mu.Unlock()
				rf.broadcastHeartbeats()
				continue
			}
			rf.mu.Unlock()
			continue
		}

		if time.Now().After(rf.electionDeadline) {
			rf.mu.Unlock()
			rf.startElection()
			continue
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) applier() {
	for !rf.killed() {
		time.Sleep(15 * time.Millisecond)
		for {
			rf.mu.Lock()
			if rf.lastApplied >= rf.commitIndex {
				rf.mu.Unlock()
				break
			}
			rf.lastApplied++
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
			rf.mu.Unlock()
			rf.applyCh <- msg
		}
	}
}

// The service or tester wants to create a Raft server. The ports
// of all the Raft servers (including this one) are in peers[]. This
// server's port is peers[me]. All the servers' peers[] arrays
// have the same order. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.me = me

	// Your initialization code here (3A, 3B).
	rf.mu.Lock()
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = Follower
	rf.applyCh = applyCh

	// index 0 placeholder so first real entry is index 1
	rf.logs = []LogEntry{{Term: 0}}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.electionDeadline = time.Now().Add(time.Duration(rand.Intn(200)+400) * time.Millisecond)
	rf.mu.Unlock()

	// start ticker goroutine to start elections.
	go rf.ticker()
	go rf.applier()

	return rf
}
