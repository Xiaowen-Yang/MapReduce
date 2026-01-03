package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// Constants for Raft states
const (
	Follower  = 0
	Candidate = 1
	Leader    = 2
)

// Struct for a single log entry
type LogEntry struct {
	Term    int         // Term when entry was received by leader
	Command interface{} // Command for state machine
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state         int       // Follower, Candidate, Leader
	lastHeartbeat time.Time // For election timeout

	// Persistent state on all servers
	currentTerm int        // Latest term server has seen
	votedFor    int        // CandidateId that received vote in current term
	log         []LogEntry //Each entry contains command for state machine

	// Volatile state on all servers
	commitIndex int // Index of highest log entry known to be committed
	lastApplied int // Index of highest log entry applied to state machine

	// Volatile state on all leader
	nextIndex  []int // For each server, index of the next log entry to send to that server
	matchIndex []int // For each server, index of highest log entry known to be replicated on server

	//2B
	applyCh   chan ApplyMsg // Used to send to the test program
	applyCond *sync.Cond    // Used to wake up the applier
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	rf.mu.Unlock()

	return term, isleader
}

// Once a new log entry is detected that has changed to a "Committed" status,
// these commands are immediately packaged and sent to the upper-layer application for execution
func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()

		// If application logs are not needed, release the lock and sleep
		// Only wake up when commitIndex > lastApplied
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}

		rf.lastApplied++
		// fmt.Printf("[Apply] Server %d applying index %d\n", rf.me, rf.lastApplied)
		commitIndex := rf.lastApplied
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[commitIndex].Command,
			CommandIndex: commitIndex,
		}
		rf.mu.Unlock()

		// Send to channel
		rf.applyCh <- msg
	}
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry (§5.4)
	LastLogTerm  int // term of candidate’s last log entry (§5.4)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}

	reply.Term = rf.currentTerm

	// Grant Vote
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isUpdateLog(args) {
		rf.state = Follower
		rf.lastHeartbeat = time.Now()
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	} else {
		reply.VoteGranted = false
	}
}

// Determine whether the log of the candidate is the latest.
func (rf *Raft) isUpdateLog(args *RequestVoteArgs) bool {
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term

	if args.LastLogTerm > lastLogTerm {
		return true
	} else if args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex {
		return true
	}
	return false
}

type AppendEntriesArgs struct {
	Term         int        // Leader's term
	LeaderId     int        // So follower can redirect clients
	PrevLogIndex int        // Index of log entry immediately preceding new ones
	PrevLogTerm  int        // Term of prevLogIndex entry
	Entries      []LogEntry // Log entries to store (empty for heartbeat)
	LeaderCommit int        // Leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // CurrentTerm, for leader to update itself
	Success bool // True if follower contained entry matching PrevLogIndex and PrevLogTerm
}

// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex
// whose term matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index
// but different terms), delete the existing entry and all that
// follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex =
// min(leaderCommit, index of last new entry)
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. Check Term
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	// Update the follower's lastHeartbeat to prevent it from timing out and becoming a candidate
	rf.currentTerm = args.Term
	rf.state = Follower
	rf.lastHeartbeat = time.Now()

	// 2. Consistency Check
	// fmt.Printf("S%d Recv AppendEntries: PrevLogIndex=%d, MyLogLen=%d\n", rf.me, args.PrevLogIndex, len(rf.log))

	// Case A: The Follower's log doesn't contain an entry at PrevLogIndex
	if args.PrevLogIndex >= len(rf.log) {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	// Case B: The Follower's log contains an entry at PrevLogIndex whose term does't match PrevLogTerm
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	// 3. The check passed, it means that PrevLogIndex was consistent before
	reply.Success = true
	reply.Term = rf.currentTerm

	// 4. Conflicts resolution
	// Iterate through args.Entries, starting from args.PrevLogIndex + 1
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + 1 + i

		if index < len(rf.log) {
			// If the position index already contains logs, check for conflicts
			if rf.log[index].Term != entry.Term {
				// Conflicts. Delete the existing entry and all that follow it
				rf.log = rf.log[:index]
				// Append new log entries
				rf.log = append(rf.log, entry)
			}
		} else {
			// If there is no log, append directly
			rf.log = append(rf.log, entry)
		}
	}

	// If the log changes, persist
	rf.persist()

	// 5. Update CommitIndex
	if args.LeaderCommit > rf.commitIndex {
		// commitIndex = min(LeaderCommit, Index of Last New Entry)
		// Last New Entry is the last log of the Follower
		if args.LeaderCommit > len(rf.log)-1 {
			rf.commitIndex = len(rf.log) - 1
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		// Wake up applier
		rf.applyCond.Broadcast()
	}
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. Only a leader can start prcessing a command
	if rf.state != Leader {
		return index, term, false
	}

	// 2. Construct the LogEntry
	entry := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}

	// 3. Append to the local log
	rf.log = append(rf.log, entry)

	index = len(rf.log) - 1
	term = rf.currentTerm
	// fmt.Printf("[Start] Leader %d got cmd at index %d term %d\n", rf.me, index, term)
	go rf.broadcastHeartbeat()

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) broadcastHeartbeat() {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		go func(server int) {
			rf.mu.Lock()
			// Check status before sending:
			// If it is no longer the Leader or the Term has changed, stop
			if rf.state != Leader || rf.currentTerm != currentTerm {
				rf.mu.Unlock()
				return
			}

			// 1. Construct parameters for this Follower

			// Calculate the index of the previous log entry
			prevLogIndex := rf.nextIndex[server] - 1

			// Boundary protection: Prevents boundary crossings
			if prevLogIndex < 0 {
				prevLogIndex = 0
			}

			// Retrieve the Term of the previous log entry
			prevLogTerm := rf.log[prevLogIndex].Term

			// Prepare the log entries to send, from nextIndex to the last
			entries := make([]LogEntry, len(rf.log)-(prevLogIndex+1))
			copy(entries, rf.log[prevLogIndex+1:])
			// fmt.Printf("Leader %d -> S%d: PrevLogIndex=%d, EntriesLen=%d\n", rf.me, server, prevLogIndex, len(entries))

			args := &AppendEntriesArgs{
				Term:         currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()

			// 2. Send RPC
			reply := &AppendEntriesReply{}
			if rf.sendAppendEntries(server, args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				// Check the status again after the RPC returns
				if rf.state != Leader || rf.currentTerm != currentTerm {
					return
				}

				// Handling Term Changes
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.votedFor = -1
					rf.persist()
					return
				}

				// 3. Process log replication results
				if reply.Success {
					newMatchIndex := args.PrevLogIndex + len(args.Entries)
					// fmt.Printf("Leader %d <- S%d: Success! Update matchIndex to %d\n", rf.me, server, newMatchIndex)
					if newMatchIndex > rf.matchIndex[server] {
						rf.matchIndex[server] = newMatchIndex
						rf.nextIndex[server] = rf.matchIndex[server] + 1
					}
					// Check if a new log can be submitted.
					rf.leaderCommitRule()
				} else {
					// Failure: Log inconsistency
					// Simple backtracking logic: decrement by 1 and retry
					// fmt.Printf("Leader %d <- S%d: Rejected (PrevLogIndex=%d). Decrementing nextIndex.\n", rf.me, server, args.PrevLogIndex)
					rf.nextIndex[server]--
					// Boundary check: Cannot be less than 1
					if rf.nextIndex[server] < 1 {
						rf.nextIndex[server] = 1
					}
				}
			}
		}(peer)
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.lastHeartbeat = time.Now()
	rf.persist()

	term := rf.currentTerm
	me := rf.me

	args := &RequestVoteArgs{
		Term:         term,
		CandidateId:  me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	rf.mu.Unlock() // Unlock after constructing parameters to allow other RPCs to process

	// Counting votes, initially vote for myself
	var votes int32 = 1

	for peer := range rf.peers {
		if peer == me {
			continue
		}

		go func(server int) {
			reply := &RequestVoteReply{}
			if rf.sendRequestVote(server, args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				// Check if our status has changed after the RPC response
				if rf.currentTerm != term || rf.state != Candidate {
					return
				}

				// Step down to follower if find a higher term
				if reply.Term > term {
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.votedFor = -1
					rf.persist()
					return
				}

				// Counting votes
				if reply.VoteGranted {
					// Atomize the vote count to avoid concurrent conflicts
					currentVotes := atomic.AddInt32(&votes, 1)

					if int(currentVotes) > len(rf.peers)/2 {
						// win and become a leader
						if rf.state == Candidate {
							rf.state = Leader
							// Initialize the Leader's volatile state
							rf.nextIndex = make([]int, len(rf.peers))
							rf.matchIndex = make([]int, len(rf.peers))
							for i := range rf.peers {
								// The initial value of nextIndex is the last log index of the Leader + 1
								rf.nextIndex[i] = len(rf.log)
								// The initial value of matchIndex is 0
								rf.matchIndex[i] = 0
							}
							rf.persist()
							go rf.broadcastHeartbeat()
						}
					}
				}
			}
		}(peer)
	}
}

// Check if commitIndex can be advanced
func (rf *Raft) leaderCommitRule() {
	if rf.state != Leader {
		return
	}

	// Algorithm: Find an N such that N > commitIndex, and for most matchesIndex[i] >= N, and log[N].Term == currentTerm

	// Start trying from commitIndex + 1 and continue until the end of the log
	for N := rf.commitIndex + 1; N < len(rf.log); N++ {
		if rf.log[N].Term != rf.currentTerm {
			continue
		}

		count := 1
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= N {
				count++
			}
		}

		if count > len(rf.peers)/2 {
			rf.commitIndex = N
			// Wake up applier
			// fmt.Printf("[Commit] Leader %d updated commitIndex to %d\n", rf.me, rf.commitIndex)
			rf.applyCond.Broadcast()
		}
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// 1. Check state
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		// 2. Action based on state
		if state == Leader {
			// Leader Logic: Send heartbeats periodically
			rf.broadcastHeartbeat()
			time.Sleep(100 * time.Millisecond)
		} else {
			// Follower/Candidate Logic: Election Timeout

			// Generate random timeout (300-600ms)
			ms := 300 + (rand.Int63() % 300)
			electionTimeout := time.Duration(ms) * time.Millisecond
			startTime := time.Now()

			// Sleep in short intervals to remain responsive
			for time.Since(startTime) < electionTimeout {
				rf.mu.Lock()
				// If we become Leader or are killed, stop waiting immediately
				if rf.state == Leader || rf.killed() {
					rf.mu.Unlock()
					goto EndOfLoop // Break out of the sleep loop, but stay in ticker
				}
				rf.mu.Unlock()
				time.Sleep(10 * time.Millisecond)
			}

			// Timeout expired, check if we should start election
			rf.mu.Lock()
			// Must verify timeout again with lastHeartbeat because a heartbeat
			// might have arrived while we were sleeping in the loop above.
			if rf.state != Leader && time.Since(rf.lastHeartbeat) >= electionTimeout {
				rf.mu.Unlock()
				rf.startElection() // startElection handles its own locking
			} else {
				rf.mu.Unlock()
			}
		}
	EndOfLoop:
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1 // -1 means "null", means that 0 is a valid ID
	rf.state = Follower
	rf.lastHeartbeat = time.Now()
	rf.log = append(rf.log, LogEntry{Term: 0}) // log starts at Index 1

	// 2B
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()
	return rf
}
