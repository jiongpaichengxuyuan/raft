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
	rf.resetElectionTimer()
	rf.becomeCandidate()
	done := false
	votes := 1
	term := rf.currentTerm
	args := RequestVoteArgs{rf.currentTerm, rf.me}

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
			if rf.currentTerm > reply.Term {
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
			rf.state = Leader
			go rf.StartAppendEntries(true)
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
}

func (rf *Raft) ToFollower() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = Follower
	rf.votedFor = None
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 竞选leader的节点任期小于等于自己的任期，则反对票
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = None
		rf.state = Follower
	}
	reply.Term = rf.currentTerm
	update := true
	if (rf.votedFor == None || rf.votedFor == args.CandidateId) && update {
		// 竞选任期大于自身任期，则更新自身任期，并转为follower
		rf.votedFor = args.CandidateId
		rf.state = Follower
		rf.resetElectionTimer() // 自己的票已经投出去了就转为Follower状态
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
}
