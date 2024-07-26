package raft

import (
	"math/rand"
	"time"
)

type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) resetVoteTimeout() {
	ms := 300 + (rand.Int63() % 500)
	rf.voteTimeout = time.Duration(ms) * time.Millisecond
	rf.lastAppendEntriesTime = time.Now()
}

func (rf *Raft) becomeLeader() {
	rf.identity = Leader
}

func (rf *Raft) becomeCandidate() {
	rf.identity = Candidate
	rf.votedNum = 1
	rf.votedFor = rf.me
	rf.currentTerm++
	rf.resetVoteTimeout()
}

func (rf *Raft) becomeFollower(term int) {
	rf.identity = Follower
	rf.votedFor = -1
	rf.currentTerm = term
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.handleSendRequestVote(reply)
	return ok
}

func (rf *Raft) handleSendRequestVote(reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 当请求投票过程中发现了新的Leader，则变为Follower
	if reply.Term > rf.currentTerm {
		// DPrintf("[server %d] Find new term, old: %d, new: %d", rf.me, rf.currentTerm, reply.Term)
		rf.becomeFollower(reply.Term)
		return
	}
	if rf.identity != Candidate {
		return
	}

	if reply.VoteGranted {
		rf.votedNum++
	}
	if rf.votedNum > len(rf.peers)/2 {
		// DPrintf("[server %d] Election success on Term %d, votedNum: %d", rf.me, rf.currentTerm, rf.votedNum)
		rf.becomeLeader()
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Raft日志只会从Leader单向传递，保证Leader的日志是最新的
	// TODO: 5.4 选举限制

	// DPrintf("[server %d] Received vote from %d, Term: %d", rf.me, args.CandidateId, args.Term)

	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	// 收到了更新的Term请求
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.lastAppendEntriesTime = time.Now()
		// DPrintf("[server %d] Voted for %d, Term: %d", rf.me, args.CandidateId, args.Term)
	}
}

func (rf *Raft) broadcastHeartBeat() {
	// DPrintf("[server %d] Begin send HeratBeat", rf.me)

	for i := 0; i < len(rf.peers); i++ {
		if rf.me == i {
			continue
		}

		args := AppendEntriesArgs{}
		args.Term = rf.currentTerm
		args.LeaderId = rf.me
		reply := AppendEntriesReply{}

		go rf.sendAppendEntries(i, &args, &reply)
	}
}

func (rf *Raft) broadcastRequestVote() {
	// DPrintf("[server %d] Begin send RequestVote", rf.me)

	for i := 0; i < len(rf.peers); i++ {
		if rf.me == i {
			continue
		}

		args := RequestVoteArgs{}
		args.Term = rf.currentTerm
		args.CandidateId = rf.me
		reply := RequestVoteReply{}

		go rf.sendRequestVote(i, &args, &reply)
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		switch rf.identity {
		case Follower:
			fallthrough
		case Candidate:
			if time.Since(rf.lastAppendEntriesTime) > rf.voteTimeout {
				// DPrintf("[server %d] Election timeout, start leader election, term: %d, timeout: %v", rf.me, rf.currentTerm+1, rf.voteTimeout)
				rf.becomeCandidate()
				rf.broadcastRequestVote()
			}
		case Leader:
			if time.Since(rf.lastHeartBeatTime) > rf.heartBeatTimeout {
				rf.broadcastHeartBeat()
				rf.lastHeartBeatTime = time.Now()
			}
		}

		rf.mu.Unlock()
		time.Sleep(time.Duration(50) * time.Millisecond)
	}
}
