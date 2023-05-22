package raft

import (
	"bytes"
	// "fmt"
	"time"

	"6.824/labgob"
)

type SnapshotArgs struct {
	Snapshot          []byte
	Entries           []Log
	LeaderCommit      int
	LastSnapShotTerm  int
	LastSnapShotIndex int
}

type SnapshotReply struct {
	MatchIndex int
}

func (rf *Raft) InstallSnapshot(args *SnapshotArgs, reply *SnapshotReply) {
	msg := ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Snapshot,
		SnapshotTerm:  args.LastSnapShotTerm,
		SnapshotIndex: args.LastSnapShotIndex,
	}
	rf.ApplyMsgChan <- msg
	rf.logs = args.Entries // 直接用leader的内容将它全部覆盖
	rf.persist()
	rf.commitIndex = args.LeaderCommit
	rf.lastApplied = 0
	rf.lastSnapShotIndex = args.LastSnapShotIndex
	rf.lastSnapShotTerm = args.LastSnapShotTerm
	reply.MatchIndex = rf.logs[len(rf.logs)-1].LogIndex
	// 别忘了在persister中保存raftstate和snapshot
	raftStateData := rf.WritePersist()
	rf.persister.SaveStateAndSnapshot(raftStateData, args.Snapshot)
	rf.CommitChan <- struct{}{}
}

func (rf *Raft) SendSnapshot(server int) {
	if rf.role != leader {
		return
	}
	RTCTimer := time.NewTimer(RPCTimeout)
	chOk := make(chan bool)

	for {
		r := bytes.NewBuffer(rf.persister.snapshot)
		d := labgob.NewDecoder(r)
		var lastIncludedIndex int
		d.Decode(&lastIncludedIndex)
		if lastIncludedIndex != rf.lastSnapShotIndex {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		args := SnapshotArgs{
			Snapshot:          rf.persister.snapshot,
			Entries:           rf.logs,
			LeaderCommit:      rf.commitIndex,
			LastSnapShotIndex: rf.lastSnapShotIndex,
			LastSnapShotTerm:  rf.lastSnapShotTerm,
		}
		reply := SnapshotReply{}
		go func() {
			ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)
			if !ok {
				time.Sleep(time.Millisecond * 10)
			}
			chOk <- ok
		}()

		select {
		case <-rf.EndChan:
			return
		case <-RTCTimer.C:
			// fmt.Printf("leader %d send snapshot to peer %d timeout\n", rf.me, server)
			return
		case ok := <-chOk:
			if !ok {
				// fmt.Printf("leader %d send snapshot to peer %d timeout\n", rf.me, server)
				continue
			}
		}
		rf.matchIndex[server] = reply.MatchIndex - rf.lastSnapShotIndex
		return
	}
}
