package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
	"github.com/lvyahui8/goenum"
)

// https://stackoverflow.com/questions/71934060/go-how-to-use-enum-as-a-type
// define enum type
type State struct {
	goenum.Enum
}

// define enum
var (
	Follower  = goenum.NewEnum[State]("Follower")
	Candidate = goenum.NewEnum[State]("Candidate")
	Leader    = goenum.NewEnum[State]("Leader")
)

// A single log entry, containing a command for the state machine
// and the term when the entry was received by the leader.
type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all servers:
	currentTerm int        // latest term server has seen
	votedFor    int        // candidateId that received vote in current term
	log         []LogEntry // log entries; each entry contains command for state machine, and

	// volatile state on all servers:
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	state           State         // current state of the server (Follower, Candidate, Leader)
	electionTimeout time.Duration // timeout for elections
	applyCh         chan raftapi.ApplyMsg

	lastContact   time.Time
	votesReceived int

	// volatile state on leaders:
	nextIndex  []int // for each server, index of the next log entry
	matchIndex []int // for each server, index of highest log entry known to be replicated
}

// RPC argument and reply structs
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

type RequestVoteReply struct {
	Term        int  // current term, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

type AppendEntriesArgs struct {
	Term         int // leader's term
	LeaderId     int // so follower can redirect clients
	PrevLogIndex int // index of log entry immediately preceding new ones
	PrevLogTerm  int // term of prevLogIndex entry
	LeaderCommit int
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching
	XTerm   int  // Term of the conflicting entry
	XIndex  int  // Index of first entry with that term
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (3A).
	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

func (rf *Raft) readPersist(data []byte) {
	if len(data) == 0 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var log []LogEntry

	// Decode all three fields in the same order they were encoded.
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		// An error here means the persisted data is corrupted.
		// For this lab, you can likely ignore it or log an error.
	} else {
		// If decoding is successful, update the Raft state.
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. Reply false if candidate's term is older.
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// 2. If we see a newer term, update ourselves and become a follower.
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.persist()
		rf.state = Follower
		rf.votedFor = -1
		rf.persist() // Persist the new term and state
	}
	reply.Term = rf.currentTerm

	// 👇 This is the new up-to-date check.
	voterLastLogIndex := len(rf.log) - 1
	voterLastLogTerm := rf.log[voterLastLogIndex].Term

	upToDate := false
	if args.LastLogTerm > voterLastLogTerm {
		upToDate = true
	}
	if args.LastLogTerm == voterLastLogTerm && args.LastLogIndex >= voterLastLogIndex {
		upToDate = true
	}

	// 3. Grant vote ONLY if we haven't voted AND candidate's log is up-to-date.
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist() // Persist the vote
		rf.lastContact = time.Now()
	} else {
		reply.VoteGranted = false
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
	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. Correctly check if we are the leader.
	if rf.state != Leader {
		return -1, -1, false // Not the leader, return immediately
	}

	// 2. Correctly create and append the new entry to our own log.
	newEntry := LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	}
	rf.log = append(rf.log, newEntry)
	// 3. Correctly persist the new log.
	rf.persist()

	// The leader's leaderLoop will now see this new entry
	// and automatically start replicating it to followers.

	index := len(rf.log) - 1
	term := rf.currentTerm
	return index, term, true
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

func (rf *Raft) ticker() {
	// In the ticker() for a follower/candidate
	for rf.killed() == false {
		rf.mu.Lock()
		timeout := rf.electionTimeout
		lastContact := rf.lastContact
		// Only check if we are not the leader
		if rf.state != Leader && time.Since(lastContact) > timeout {
			// Timeout has elapsed, so we must start an election.
			// Release the lock BEFORE calling startElection.
			rf.mu.Unlock()
			rf.startElection()

			// Since startElection will run and then the loop continues,
			// we need to skip the outer unlock. A 'continue' is good here.
			continue
		}
		rf.mu.Unlock()

		time.Sleep(10 * time.Millisecond) // Check every 10ms
	}
}

func (rf *Raft) startElection() {
	// Transition to candidate state, increment term, vote for self,
	// send RequestVote RPCs to all other servers.
	// (This function should be called when the election timeout is reached.)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Increment term and change state to Candidate
	rf.currentTerm++
	rf.persist() // Persist the new term and vote
	rf.votedFor = rf.me
	rf.persist() // Persist the new term and vote
	rf.state = Candidate
	rf.lastContact = time.Now()
	rf.votesReceived = 1 // We vote for ourselves

	// Reset election timeout
	rf.electionTimeout = time.Duration(rand.Intn(150)+150) * time.Millisecond

	// Send RequestVote RPCs to all peers
	for i := range rf.peers {
		if i != rf.me {
			// You'll need to handle the case where the log is empty.
			lastLogIndex := len(rf.log) - 1
			lastLogTerm := 0 // Default to 0 if log is empty
			if lastLogIndex >= 0 {
				lastLogTerm = rf.log[lastLogIndex].Term
			}

			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			// reply := &RequestVoteReply{}
			go func(server int, args RequestVoteArgs) { // Pass args by value
				reply := &RequestVoteReply{}
				if rf.sendRequestVote(server, &args, reply) {
					rf.handleVoteReply(&args, reply) // You may want to pass args here too
				}
			}(i, args) // Send a copy of args to the goroutine
		}
	}
}

func (rf *Raft) handleVoteReply(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Lock the mutex
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Candidate {
		return
	}

	if reply.Term > rf.currentTerm {
		// revert to being a Follower
		rf.state = Follower
		// update your currentTerm
		rf.currentTerm = reply.Term
		rf.persist() // Persist the new term and state
		// Reset votedFor to "null" (-1)
		rf.votedFor = -1
		rf.persist() // Persist the new term and state
		return
	}
	if reply.VoteGranted {
		// If the reply is a vote granted and we are still a candidate,
		// increment our vote count.
		rf.votesReceived++
		if rf.votesReceived > len(rf.peers)/2 {
			// You won the election! 🎉
			rf.state = Leader

			// 👇 Initialize leader state here, just once.
			rf.nextIndex = make([]int, len(rf.peers))
			rf.matchIndex = make([]int, len(rf.peers))
			for i := range rf.peers {
				rf.nextIndex[i] = len(rf.log)
			}

			// Now that you're the leader, your main job is to
			// send heartbeats, not wait for an election timeout.
			// You would typically start a separate heartbeat-sending
			// goroutine here.
			go rf.leaderLoop() // Start the leader's work
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) handleAppendEntriesReply(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader || rf.currentTerm != args.Term {
		return
	}

	if reply.Success {
		// ... (This part of your code is correct)
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		rf.updateLeaderCommitIndex()

	} else {
		// Handle failed AppendEntries
		if reply.Term > rf.currentTerm {
			// Step down if we find a peer with a higher term
			rf.currentTerm = reply.Term
			rf.persist()
			rf.state = Follower
			rf.votedFor = -1
			rf.persist() // Persist the new term and vote
		} else {
			// This is the log reconciliation optimization

			// Search backwards for the last log entry with the follower's conflicting term.
			lastLogIndexWithXTerm := -1
			for i := len(rf.log) - 1; i >= 0; i-- {
				if rf.log[i].Term == reply.XTerm {
					lastLogIndexWithXTerm = i
					break
				}
			}

			if lastLogIndexWithXTerm != -1 {
				// Case 1: Leader has the conflicting term. Set nextIndex to be
				// one past the leader's last entry for that term.
				rf.nextIndex[server] = lastLogIndexWithXTerm + 1
			} else {
				// Case 2: Leader does not have the conflicting term.
				// Set nextIndex to the first index of that term on the follower.
				// (The follower has already provided this in reply.XIndex)
				rf.nextIndex[server] = reply.XIndex
			}
		}
	}
}

// updateLeaderCommitIndex checks if a majority of followers have replicated
// an entry, allowing the leader to commit it.
func (rf *Raft) updateLeaderCommitIndex() {
	// Loop backwards from the end of the log.
	for N := len(rf.log) - 1; N > rf.commitIndex; N-- {
		// An entry can only be committed if it's from the leader's current term.
		if rf.log[N].Term != rf.currentTerm {
			continue
		}

		// Count how many peers (including self) have this entry.
		count := 1
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= N {
				count++
			}
		}

		// If a majority has the entry, we can commit it.
		if count > len(rf.peers)/2 {
			rf.commitIndex = N
			// The separate applier goroutine will now see this
			// new commitIndex and apply the entry.
			break // We found the highest possible new commitIndex.
		}
	}
}

func (rf *Raft) leaderLoop() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}

		// Send AppendEntries to all followers in a burst.
		for i := range rf.peers {
			if i == rf.me {
				continue
			}

			// Determine which entries to send (if any).
			nextIdx := rf.nextIndex[i]
			prevLogIndex := nextIdx - 1
			if prevLogIndex < 0 {
				// Handle this edge case, perhaps by sending a snapshot later.
				// For now, you might just skip this peer for this round.
				continue
			}
			prevLogTerm := rf.log[prevLogIndex].Term
			var entriesToSend []LogEntry

			if len(rf.log) > nextIdx {
				entriesToSend = rf.log[nextIdx:]
			}

			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entriesToSend, // Will be nil for a heartbeat
				LeaderCommit: rf.commitIndex,
			}

			go func(server int, args AppendEntriesArgs) {
				reply := AppendEntriesReply{}
				if rf.sendAppendEntries(server, &args, &reply) {
					rf.handleAppendEntriesReply(server, &args, &reply)
				}
			}(i, args)
		}

		// Unlock *after* sending RPCs to all peers.
		rf.mu.Unlock()

		// Sleep *after* a full round of heartbeats.
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Rule #1: Reply false if request's term is older than ours.
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// ... (Term handling and state updates as before) ...
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.persist()
		rf.votedFor = -1
		rf.persist() // Persist the new term and vote
	}
	rf.state = Follower
	rf.lastContact = time.Now()
	reply.Term = rf.currentTerm

	// Rule #2: Log Consistency Check
	if args.PrevLogIndex >= len(rf.log) {
		// Case 1: Follower's log is too short.
		reply.XTerm = -1           // No conflicting term
		reply.XIndex = len(rf.log) // Tell leader our actual log length
		reply.Success = false
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// Case 2: Term mismatch at PrevLogIndex.
		conflictingTerm := rf.log[args.PrevLogIndex].Term
		reply.XTerm = conflictingTerm

		// Search backwards for the first index of the conflicting term.
		firstIndexOfTerm := args.PrevLogIndex
		for firstIndexOfTerm > 0 && rf.log[firstIndexOfTerm-1].Term == conflictingTerm {
			firstIndexOfTerm--
		}
		reply.XIndex = firstIndexOfTerm
		reply.Success = false
		return
	}
	// Rule #3 & #4: Handle conflicting entries and append new ones.
	// This simple "truncate and append" approach is a common and effective
	// way to implement this logic.
	rf.log = rf.log[:args.PrevLogIndex+1]
	rf.persist() // Persist after modifying the log
	rf.log = append(rf.log, args.Entries...)
	rf.persist() // Persist after modifying the log

	// Rule #5: Update commitIndex.
	if args.LeaderCommit > rf.commitIndex {
		// The min() is to prevent commitIndex from going past the end of our log.
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	}

	reply.Success = true
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
// Make creates a new Raft server.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {

	// 1. Initialize the Raft struct with default values.
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh // Make sure to store the applyCh

	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1

	// Initialize the log with a single dummy entry at index 0.
	// This simplifies the logic for log replication.
	rf.log = []LogEntry{{Term: 0}}

	rf.commitIndex = 0
	rf.lastApplied = 0

	// Set a randomized election timeout to start.
	rf.lastContact = time.Now()
	rf.electionTimeout = time.Duration(150+rand.Intn(150)) * time.Millisecond

	// 2. Restore state from the persister.
	// This will overwrite the default values if the server is restarting.
	rf.readPersist(persister.ReadRaftState())

	// 3. Start the background goroutines.
	// The ticker handles election timeouts for followers/candidates.
	go rf.ticker()
	// The applier sends committed log entries to the service.
	go rf.applier()

	return rf
}

// applier is a long-running goroutine that checks for newly committed
// log entries and sends them to the service on the applyCh.
func (rf *Raft) applier() {
	for !rf.killed() {
		var messagesToApply []raftapi.ApplyMsg

		rf.mu.Lock()
		// If the commitIndex has advanced past the lastApplied index,
		// there are new entries to apply.
		if rf.commitIndex > rf.lastApplied {
			// Collect all entries from lastApplied up to commitIndex.
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				msg := raftapi.ApplyMsg{
					CommandValid: true,
					Command:      rf.log[i].Command,
					CommandIndex: i,
				}
				messagesToApply = append(messagesToApply, msg)
			}
			// Update lastApplied to reflect the new state.
			rf.lastApplied = rf.commitIndex
		}
		rf.mu.Unlock() // IMPORTANT: Release the lock before sending on the channel.

		// Send the collected messages to the service.
		// This happens outside the lock to prevent deadlocks if the service is slow.
		for _, msg := range messagesToApply {
			rf.applyCh <- msg
		}

		// Pause briefly to avoid busy-waiting.
		time.Sleep(10 * time.Millisecond)
	}
}
