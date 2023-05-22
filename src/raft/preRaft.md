// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
// heartbeat time span: 200ms
// least ticker time: 1000ms
// max ticker time: 1500ms
// candidate ticker time gap: 100ms
// const heartBeatInterval = 110 * 1000
// const serverVoteInterval = 0.5 * 1000
// const floorTime = 300 * 1000
// const ceilTime = 450 * 1000

// func (rf *Raft) GetRandTime() int {
// 	// randomly get n as interger in [1000, 1500]
// 	n := (rand.Intn(ceilTime-floorTime+1) + floorTime) / serverVoteInterval
// 	return n
// }

// func (rf *Raft) ticker() {
// 	isleader := false
// 	for !rf.killed() {
// 		n := rf.GetRandTime()
// 		for i := 0; i < n; i++ {
// 			if rf.heartBeat { // if get heartbeat, reset the time
// 				if !rf.isLeader {
// 					fmt.Printf("peer %d get a heartbeat from leader %d\n", rf.me, rf.leaderId)
// 				}
// 				break
// 			} else {
// 				time.Sleep(time.Microsecond * time.Duration(serverVoteInterval)) // scan heartbeat state every 1ms
// 			}
// 		}
// 		if rf.isLeader {
// 			isleader = true
// 		} else if !rf.isLeader && isleader {
// 			fmt.Printf("peer %d nolonger leader\n", rf.me)
// 			isleader = false
// 		}
// 		if rf.heartBeat && !rf.isLeader { // "break" to there, reset ticker
// 			rf.heartBeat = false
// 		} else if rf.heartBeat && rf.isLeader { // leader don't need ticker
// 			rf.heartBeat = true
// 		} else if !rf.isLeader && !rf.heartBeat { // have done "for", start vote
// 			// request candidate rpc
// 			fmt.Printf("peer %d start a vote... \n", rf.me)
// 			fmt.Printf("peer %d ticker time: %d\n", rf.me, n)
// 			if !rf.isLeader {
// 				rf.RaftVote()
// 			}
// 			fmt.Printf("peer %d isleader: %v\n", rf.me, rf.isLeader)
// 		} else {
// 			continue
// 		}

// 		// Your code here to check if a leader election should
// 		// be started and to randomize sleeping time using
// 		// time.Sleep().

// 	}
// }

//
// server's ticker outtime, it need to start a vote
//
// func (rf *Raft) RaftVote() {
// 	rf.role = candidate
// 	rf.numVote = 0
// 	term := rf.currentTerm
// 	RequestVoteArgs := [20]RequestVoteArgs{}
// 	RequestVoteReply := [20]RequestVoteReply{}
// 	// ok := [20]bool{}
// 	startTime := time.Now()
// 	rf.numVote++ // vote for itself firstly
// 	for i := 0; i < rf.numServer; i++ {
// 		if rf.currentTerm != term {
// 			return
// 		}
// 		if i != rf.me {
// 			fmt.Printf("peer %d send vote...\n", rf.me)
// 			RequestVoteArgs[i].Term = rf.currentTerm
// 			RequestVoteArgs[i].CandidateId = rf.me
// 			RequestVoteArgs[i].LastLogIndex = rf.lastLogIndex
// 			RequestVoteArgs[i].LastLogTerm = rf.lastLogTerm
// 			if ok := rf.sendRequestVote(i, &RequestVoteArgs[i], &RequestVoteReply[i]); ok {
// 				if RequestVoteReply[i].HasVoted {
// 					fmt.Printf("%d gives %d a request vote!\n", i, rf.me)
// 					rf.numVote++
// 				}
// 			} else {
// 				fmt.Printf("%d to %d send request vote failed!\n", rf.me, i)
// 			}
// 		}
// 	}
// 	now := time.Now()
// 	fmt.Println("vote rpc need time: ", now.Sub(startTime))
// 	rf.mu.Lock()
// 	if rf.currentTerm != term {
// 		return
// 	}
// 	MaxHalf := int(math.Floor(float64(rf.numServer) / 2.0))
// 	if rf.numVote > MaxHalf {
// 		fmt.Printf("%d vote success!\n", rf.me)
// 		rf.isLeader = true
// 		rf.role = leader
// 		rf.leaderId = rf.me
// 		rf.currentTerm++
// 		rf.votedFor = rf.me
// 		rf.heartBeat = true // leader don't need ticker
// 	} else {
// 		fmt.Printf("%d vote failed!\n", rf.me)
// 		rf.isLeader = false
// 		rf.role = follower
// 		rf.heartBeat = false // reset ticker
// 	}
// 	rf.mu.Unlock()
// }

//
// send heartbeat
//
// func (rf *Raft) RaftLeaderHeartBeat() {
// 	AppendEntries := [20]AppendEntries{}
// 	AppendEntriesReply := [20]AppendEntriesReply{}
// 	for !rf.killed() {
// 		// MaxHalf := int(math.Floor(float64(rf.numServer) / 2.0))
// 		// NumSuccessSend := 1
// 		if rf.isLeader {
// 			for i := 0; i < rf.numServer; i++ {
// 				if i != rf.me {
// 					AppendEntries[i].Term = rf.currentTerm
// 					AppendEntries[i].LeaderId = rf.me
// 					AppendEntries[i].Entries.IsHeartBeat = true
// 					if ok := rf.SendHeartBeat(i, &AppendEntries[i], &AppendEntriesReply[i]); ok {
// 						if AppendEntriesReply[i].HasRecevied {
// 							// NumSuccessSend++
// 							fmt.Printf("leader %d sends %d a HeartBeat!\n", rf.me, i)
// 						} else {
// 							fmt.Printf("leader %d sends %d is not a HeartBeat!\n", rf.me, i)
// 						}
// 					} else {
// 						fmt.Printf("%d to %d send heartbeat failed!\n", rf.me, i)
// 					}
// 				}
// 			}
// 			// if NumSuccessSend > MaxHalf {
// 			// 	rf.heartBeat = true
// 			// }
// 			time.Sleep(time.Microsecond * heartBeatInterval)
// 		}
// 	}

// }