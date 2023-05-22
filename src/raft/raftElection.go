package raft

import (
	// "fmt"
	"math/rand"
	"time"
)

type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int //  candidate's id
	LastLogIndex int // candidate's last log id
	LastLogTerm  int // candidate's last log term
	// Your data here (2A, 2B).
}

type RequestVoteReply struct {
	HasVoted bool
	Term     int
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.HasVoted = false
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.ChangeRole(follower)
	}
	lastLogTerm, lastLogIndex := rf.GetLastLogTermIndex()
	if args.LastLogTerm < lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
		reply.HasVoted = false
		return
	}

	// 获得认可
	reply.HasVoted = true
	rf.votedFor = args.CandidateId
	rf.currentTerm = args.Term
	rf.ChangeRole(follower)
	rf.ResetElectionTimer()
}

func (rf *Raft) StartElection() {
	rf.ElectionTimer.Stop()
	rf.ElectionTimer.Reset(RandomElectionTime())

	if rf.role == leader {
		return
	}

	rf.role = candidate
	lastLogTerm, lastLogIndex := rf.GetLastLogTermIndex()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	reply := RequestVoteReply{}

	voteCh := make(chan RequestVoteReply, rf.numServer)
	for i := 0; i < rf.numServer; i++ {
		if i == rf.me {
			continue
		}
		go func(server int, args RequestVoteArgs, reply RequestVoteReply) {
			RPCTimer := time.NewTimer(RPCTimeout)
			chReply := make(chan RequestVoteReply, 1)
			chOk := make(chan bool, 1)
			for {
				go func(server int, args RequestVoteArgs, reply RequestVoteReply) {
					ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)
					if !ok {
						time.Sleep(time.Millisecond * 10)
					}
					chReply <- reply
					chOk <- ok
				}(server, args, reply)
				select {
				case <-RPCTimer.C:
					reply := RequestVoteReply{Term: -1, HasVoted: false}
					voteCh <- reply
					return
				case ok := <-chOk:
					if ok { // call 收到消息
						reply := <-chReply
						voteCh <- reply
						return
					} else {
						// call 没有收到消息，重新发送
						continue
					}
				}
			}
		}(i, args, reply)
	}

	numVote := 1
	numNews := 1
	for { // 开始等待协程消息传回，并点票
		r := <-voteCh
		numNews++
		if r.HasVoted {
			numVote++
		}
		if r.Term > rf.currentTerm {
			rf.ChangeRole(follower)
			return
		}
		if (numNews == rf.numServer) || (numVote > rf.numServer/2) || (numNews-numVote > rf.numServer/2) {
			break
		}
	}

	if numVote > rf.numServer/2 {
		rf.ChangeRole(leader)
		return
	} else {
		rf.ChangeRole(follower)
		return
	}
}

func (rf *Raft) ChangeRole(role int) {
	switch role {
	case follower:
		rf.role = follower
	case candidate:
		rf.role = candidate
		rf.ResetElectionTimer()
		rf.votedFor = rf.me
	case leader:
		rf.role = leader
		rf.votedFor = rf.me
		rf.currentTerm++
		for i := 0; i < rf.numServer; i++ {
			// 成为leader后同步所有follower的matchIndex
			// 默认为leader.log的最后一个log的位置
			rf.matchIndex[i] = len(rf.logs) - 1
		}
		rf.ResetAppendEntryTimers() // 立即发送entry给所有peers
	}
}

func (rf *Raft) ResetElectionTimer() {
	rf.ElectionTimer.Stop()
	rf.ElectionTimer.Reset(RandomElectionTime())
}

func RandomElectionTime() time.Duration {
	r := time.Duration(rand.Int63()) % ElectionTimeOffset
	return ElectionTimeout + r
}
