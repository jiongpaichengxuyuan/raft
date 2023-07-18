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
	"fmt"
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

// 全局参数
const Follower, Candidate, Leader = 1, 2, 3     // 节点的三种状态
const tickInterval = 70 * time.Millisecond      //
const heartbeatTimeout = 150 * time.Millisecond //心跳超时

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
	state            int           // 节点状态，Candidate-Follower-Leader
	currentTerm      int           // 当前的任期
	votedFor         int           // 投票给谁
	heartbeatTimeout time.Duration // 心跳定时器
	electionTimeout  time.Duration //选举计时器
	lastElection     time.Time     // 上一次的选举时间，用于配合since方法计算当前的选举时间是否超时
	lastHeartbeat    time.Time     // 上一次的心跳时间，用于配合since方法计算当前的心跳时间是否超时
	peerTrackers     []PeerTracker // keeps track of each peer's next index, match index, etc.
	log              *Log          //日志记录

	// Volatile state
	commitIndex int // 本机提交
	lastApplied int // 该日志在所有的机器上跑一边后更新

	applyHelper *ApplyHelper
	applyCond   *sync.Cond
}

type RequestAppendEntriesArgs struct {
	LeaderTerm   int // Leader的Term
	LeaderId     int // Leader的Id
	PrevLogIndex int // 新日志条目的上一个日志的索引
	PrevLogTerm  int // 新日志的上一个日志的任期
	// Logs         []ApplyMsg // 需要被保存的日志条目，可能有多个
	Entries      []Entry //
	LeaderCommit int     // Leader已提交的最高的日志项目的索引
}

type RequestAppendEntriesReply struct {
	FollowerTerm int  // Follower的Term，给Leader更新自己的Term
	Success      bool // 是否推送成功
	PrevLogIndex int
	PrevLogTerm  int
}

func (rf *Raft) getHeartbeatTime() time.Duration {
	return time.Millisecond * 110
}

// 随机化的选举超时时间
func (rf *Raft) getElectionTime() time.Duration {
	return time.Millisecond * time.Duration(350+rand.Intn(200))
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader
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
	Term        int // canditate's term
	CandidateId int // candidate requesting vote

	LastLogIndex int // index of candidate's last log entry (&5.4)
	LastLogTerm  int // term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
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
func (rf *Raft) sendRequestAppendEntries(isHeartbeat bool, server int, args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) bool {
	var ok bool
	if isHeartbeat {
		ok = rf.peers[server].Call("Raft.HandleHeartbeatRPC", args, reply)
	} else {
		ok = rf.peers[server].Call("Raft.HandleAppendEntriesRPC", args, reply)
	}
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
	term = rf.currentTerm
	if rf.state != Leader {
		isLeader = false
		return index, term, isLeader
	}
	index = rf.log.LastLogIndex + 1
	rf.log.appendL(Entry{term, command})
	go rf.StartAppendEntries(false)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.applyHelper.Kill()
	DPrintf(111, "%v : is killed!!", rf.SayMeL())
	DPrintf(111, "%v : my applyHelper is killed!!", rf.SayMeL())
	rf.state = Follower
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) StartAppendEntries(heart bool) {
	// 所有节点共享同一份request参数
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return
	}
	rf.resetElectionTimer()
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.AppendEntries(i, heart)
	}
}

func (rf *Raft) AppendEntries(targetServerId int, heart bool) {
	if heart {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		reply := RequestAppendEntriesReply{}
		args := RequestAppendEntriesArgs{}
		args.LeaderTerm = rf.currentTerm
		args.LeaderId = rf.me
		DPrintf(111, "%v: %d is a leader, ready sending heartbeart to follower %d....", rf.SayMeL(), rf.me, targetServerId)
		rf.mu.Unlock()

		ok := rf.sendRequestAppendEntries(true, targetServerId, &args, &reply)
		if !ok {
			return
		}
		if reply.Success {
			return
		}
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		if reply.FollowerTerm < rf.currentTerm {
			rf.mu.Unlock()
			return
		}
		// 拒绝接收心跳，则可能是因为任期导致的
		if reply.FollowerTerm > rf.currentTerm {
			rf.votedFor = None
			rf.state = Follower
			rf.currentTerm = reply.FollowerTerm
		}
		rf.mu.Unlock()
		return
	} else {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		args := RequestAppendEntriesArgs{}
		args.PrevLogIndex = min(rf.log.LastLogIndex, rf.peerTrackers[targetServerId].nextIndex-1)
		if args.PrevLogIndex+1 < rf.log.FirstLogIndex {
			DPrintf(111, "此时 %d 节点的nextIndex为%d,LastLogIndex为 %d, 最后一项日志为：\n", rf.me, rf.peerTrackers[rf.me].nextIndex,
				rf.log.LastLogIndex)
			return
		}
		args.LeaderTerm = rf.currentTerm
		args.LeaderId = rf.me
		args.LeaderCommit = rf.commitIndex
		args.PrevLogTerm = rf.getEntryTerm(args.PrevLogIndex)
		args.Entries = rf.log.getAppendEntries(args.PrevLogIndex + 1)
		DPrintf(111, "%v: the len of log entries: %d is ready to send to node %d!!! and the entries are %v\n",
			rf.SayMeL(), len(args.Entries), targetServerId, args.Entries)
		rf.mu.Unlock()

		reply := RequestAppendEntriesReply{}

		ok := rf.sendRequestAppendEntries(false, targetServerId, &args, &reply)
		if !ok {
			return
		}
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state != Leader {
			return
		}

		// 丢弃旧的rpc响应
		if reply.FollowerTerm < rf.currentTerm {
			return
		}
		DPrintf(111, "%v: get reply from %v reply.Term=%v reply.Success=%v reply.PrevLogTerm=%v reply.PrevLogIndex=%v myinfo:rf.log.FirstLogIndex=%v rf.log.LastLogIndex=%v\n",
			rf.SayMeL(), targetServerId, reply.FollowerTerm, reply.Success, reply.PrevLogTerm, reply.PrevLogIndex, rf.log.FirstLogIndex, rf.log.LastLogIndex)
		if reply.FollowerTerm > rf.currentTerm {
			rf.state = Follower
			rf.currentTerm = reply.FollowerTerm
			rf.votedFor = None
			return
		}
		DPrintf(111, "%v: get append reply reply.PrevLogIndex=%v reply.PrevLogTerm=%v reply.Success=%v heart=%v\n", rf.SayMeL(), reply.PrevLogIndex, reply.PrevLogTerm, reply.Success, heart)
		if reply.Success {
			rf.peerTrackers[targetServerId].nextIndex = args.PrevLogIndex + len(args.Entries) + 1
			rf.peerTrackers[targetServerId].matchIndex = args.PrevLogIndex + len(args.Entries)
			DPrintf(111, "success! now trying to commit the log...\n")
			rf.tryCommitL(rf.peerTrackers[targetServerId].matchIndex)
			return
		}

		// reply.Success is false
		if rf.log.empty() {
			return
		}
		if reply.PrevLogIndex+1 < rf.log.FirstLogIndex {
			return
		}

		if reply.PrevLogIndex > rf.log.LastLogIndex {
			rf.peerTrackers[targetServerId].nextIndex = rf.log.LastLogIndex + 1
		} else if rf.getEntryTerm(reply.PrevLogIndex) == reply.PrevLogTerm {
			rf.peerTrackers[targetServerId].nextIndex = reply.PrevLogIndex + 1
		} else {
			PrevIndex := reply.PrevLogIndex
			for PrevIndex >= rf.log.FirstLogIndex && rf.getEntryTerm(PrevIndex) == rf.getEntryTerm(reply.PrevLogIndex) {
				PrevIndex--
			}
			rf.peerTrackers[targetServerId].nextIndex = PrevIndex + 1
		}
	}

}

func (rf *Raft) SayMeL() string {
	return fmt.Sprintf("[Server %v as %v at term %v]", rf.me, rf.state, rf.currentTerm)
}

func (rf *Raft) sendMsgToTester() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		DPrintf(11, "%v: it is being blocked...", rf.SayMeL())
		rf.applyCond.Wait()

		for rf.lastApplied+1 <= rf.commitIndex {
			i := rf.lastApplied + 1
			rf.lastApplied++
			if i < rf.log.FirstLogIndex {
				DPrintf(111, "%v: apply index=%v but rf.log.FirstLogIndex=%v rf.lastApplied=%v\n",
					rf.SayMeL(), i, rf.log.FirstLogIndex, rf.lastApplied)
				panic("error happening")
			}
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log.getOneEntry(i).Command,
				CommandIndex: i,
			}
			DPrintf(111, "%s: next apply index=%v lastApplied=%v len entries=%v "+
				"LastLogIndex=%v cmd=%v\n", rf.SayMeL(), i, rf.lastApplied, len(rf.log.Entries),
				rf.log.LastLogIndex, rf.log.getOneEntry(i).Command)
			rf.applyHelper.tryApply(&msg)
		}
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (2A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		switch state {
		case Follower:
			fallthrough
		case Candidate:
			if rf.pastElectionTimeout() {
				rf.StartElection()
			}
		case Leader:
			isHeartbeat := false
			if rf.pastHeartbeatTimeout() {
				isHeartbeat = true
				rf.resetHeartbeatTimer()
				rf.StartAppendEntries(isHeartbeat)
			}
			rf.StartAppendEntries(isHeartbeat)
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		time.Sleep(tickInterval)
	}
	DPrintf(111, "tim")
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
	rf.votedFor = None
	rf.state = Follower
	rf.heartbeatTimeout = heartbeatTimeout
	rf.resetElectionTimer()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.log = NewLog()
	rf.applyHelper = NewApplyHelper(applyCh, rf.lastApplied)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.peerTrackers = make([]PeerTracker, len(rf.peers))
	rf.applyCond = sync.NewCond(&rf.mu)

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.sendMsgToTester() // 供config协程追踪日志以测试
	return rf
}

func (rf *Raft) getLastEntryTerm() int {
	if rf.log.LastLogIndex >= rf.log.FirstLogIndex {
		return rf.log.getOneEntry(rf.log.LastLogIndex).Term
	}

	return -1

}
