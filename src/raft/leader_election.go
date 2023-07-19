package raft

import (
	"math/rand"
	"time"
)

const baseElectionTimeout = 300
const None = -1

func (rf *Raft) StartElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Leader {
		return
	}
	rf.becomeCandidate()
	done := false
	votes := 1
	term := rf.currentTerm
	args := RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.log.LastLogIndex
	args.LastLogTerm = rf.getLastEntryTerm()
	defer rf.persist()
	for i, _ := range rf.peers {
		if rf.me == i {
			continue
		}
		// 开启协程去尝试拉去选票
		go func(serverId int) {
			var reply RequestVoteReply
			ok := rf.sendRequestVote(serverId, &args, &reply)
			if !ok || !reply.VoteGranted {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term < rf.currentTerm {
				return
			}
			votes++
			if done || votes <= len(rf.peers)/2 {
				return
			}
			done = true
			if rf.state != Candidate || rf.currentTerm != term {
				return
			}
			rf.becomeLeader()
			// go rf.StartAppendEntries(true)
		}(i)
	}
}

func (rf *Raft) pastElectionTimeout() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	f := time.Since(rf.lastElection) > rf.electionTimeout
	return f
}

func (rf *Raft) resetElectionTimer() {
	electionTimeout := baseElectionTimeout + (rand.Int63() % baseElectionTimeout)
	rf.electionTimeout = time.Duration(electionTimeout) * time.Millisecond
	rf.lastElection = time.Now()
}

func (rf *Raft) becomeCandidate() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.resetElectionTimer()
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	DPrintf(100, "%v :becomes leader and reset TrackedIndex\n", rf.SayMeL())
	rf.resetTrackedIndex()

}

func (rf *Raft) ToFollower() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = Follower
	rf.votedFor = None
}

// 定义一个心跳兼日志同步处理器，这个方法是Candidate和Follower节点的处理
func (rf *Raft) HandleHeartbeatRPC(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.FollowerTerm = rf.currentTerm
	reply.Success = true
	// 旧任期的leader抛弃掉
	if args.LeaderTerm < rf.currentTerm {
		reply.FollowerTerm = rf.currentTerm
		reply.Success = false
		return
	}
	DPrintf(200, "%v: I am now receiving heartbeat from leader %d at term %d", rf.SayMeL(), args.LeaderId, args.LeaderTerm)
	rf.resetElectionTimer()
	rf.state = Follower
	rf.votedFor = args.LeaderId

	// 需要转变自己的身份为Follower
	// 承认来者是个合法的新的Leader，则任期一定大于自己，此时需要设置votedFor为-1
	if args.LeaderTerm > rf.currentTerm {
		rf.votedFor = None
		rf.currentTerm = args.LeaderTerm
		reply.FollowerTerm = rf.currentTerm // 将更新了的任期传给主节点
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteGranted = true
	reply.Term = rf.currentTerm
	// 竞选leader的节点任期小于等于自己的任期，则反对票
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		rf.persist()
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.votedFor = None
		rf.state = Follower
	}

	// candidate节点发送过来的日志索引以及任期必须大于等于自己的日志索引及任期
	update := false
	update = update || args.LastLogTerm > rf.getLastEntryTerm()
	update = update || args.LastLogTerm == rf.getLastEntryTerm() && args.LastLogIndex >= rf.log.LastLogIndex
	if (rf.votedFor == None || rf.votedFor == args.CandidateId) && update {
		// 竞选任期大于自身任期，则更新自身任期，并转为follower
		rf.votedFor = args.CandidateId
		rf.state = Follower
		rf.resetElectionTimer() // 自己的票已经投出去了就转为Follower状态
		rf.persist()
	} else {
		reply.VoteGranted = false
	}
}
