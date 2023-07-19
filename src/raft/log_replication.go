package raft

import "time"

func (rf *Raft) pastHeartbeatTimeout() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return time.Since(rf.lastHeartbeat) > rf.heartbeatTimeout
}

func (rf *Raft) resetHeartbeatTimer() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastHeartbeat = time.Now()
}

func (rf *Raft) HandleAppendEntriesRPC(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.FollowerTerm = rf.currentTerm
	reply.Success = true
	// 旧任期的Leader抛弃掉
	if args.LeaderTerm < rf.currentTerm {
		reply.Success = false
		return
	}
	rf.resetElectionTimer()
	rf.state = Follower

	if args.LeaderTerm > rf.currentTerm {
		rf.votedFor = None
		rf.currentTerm = args.LeaderTerm
		reply.FollowerTerm = rf.currentTerm
	}
	defer rf.persist()
	if args.PrevLogIndex+1 < rf.log.FirstLogIndex || args.PrevLogIndex > rf.log.LastLogIndex {
		reply.FollowerTerm = rf.currentTerm
		reply.Success = false
		reply.PrevLogIndex = rf.log.LastLogIndex
		reply.PrevLogTerm = rf.getLastEntryTerm()
	} else if rf.getEntryTerm(args.PrevLogIndex) == args.PrevLogTerm {
		ok := true
		for i, entry := range args.Entries {
			index := args.PrevLogIndex + 1 + i
			if index > rf.log.LastLogIndex {
				rf.log.appendL(entry)
			} else if rf.log.getOneEntry(index).Term != entry.Term {
				ok = false
				*rf.log.getOneEntry(index) = entry
			}
		}
		if !ok {
			rf.log.LastLogIndex = args.PrevLogIndex + len(args.Entries)
		}
		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit < rf.log.LastLogIndex {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = rf.log.LastLogIndex
			}
			rf.applyCond.Broadcast()
		}
		reply.FollowerTerm = rf.currentTerm
		reply.Success = true
		reply.PrevLogIndex = rf.log.LastLogIndex
		reply.PrevLogTerm = rf.getLastEntryTerm()
	} else {
		prevIndex := args.PrevLogIndex
		for prevIndex >= rf.log.FirstLogIndex && rf.getEntryTerm(prevIndex) == rf.log.getOneEntry(args.PrevLogIndex).Term {
			prevIndex--
		}
		reply.FollowerTerm = rf.currentTerm
		reply.Success = false
		if prevIndex >= rf.log.FirstLogIndex {
			reply.PrevLogIndex = prevIndex
			reply.PrevLogTerm = rf.getEntryTerm(prevIndex)
		}
	}
}

func (rf *Raft) tryCommitL(matchIndex int) {
	if matchIndex <= rf.commitIndex {
		// 首先matchIndex应该是大于leader节点的commitIndex才能提交，因为commitIndex及其之前的不需要更新
		return
	}
	// 越界的也不能提交
	if matchIndex > rf.log.LastLogIndex {
		return
	}
	if matchIndex < rf.log.FirstLogIndex {
		return
	}

	//提交的必须本任期内从客户端收到日志
	if rf.getEntryTerm(matchIndex) != rf.currentTerm {
		return
	}

	// 计算所有已经正确匹配该matchIndex的从节点的票数
	cnt := 1
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		if matchIndex <= rf.peerTrackers[i].matchIndex {
			cnt++
		}
	}
	if cnt > len(rf.peers)/2 {
		rf.commitIndex = matchIndex
		rf.applyCond.Broadcast()
	}
}
