package raft

// Possible values for StateType.
const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

// StateType represents the role of a node in a cluster.
type StateType int

var stMap = [...]string{
	"FOLLOWER",
	"CANDIDATE",
	"LEADER",
}

func (st StateType) String() string {
	return stMap[st]
}

func (rf *Raft) becomeLeader() {
	rf.state = StateLeader
	rf.electionTimer.Stop()                                 //停止竞举定时器
	rf.Broadcast(true)                                      //广播心跳
	resetTimer(rf.heartbeatTimer, StableHeartbeatTimeout()) //重置心跳超时时间
	DPrintf("[Node %v][Term %v] from %v to LEADER", rf.me, rf.currentTerm, rf.state)
}
func (rf *Raft) becomeFollower() {
	rf.state = StateFollower
	rf.heartbeatTimer.Stop()                                  //停止发送心跳
	resetTimer(rf.electionTimer, RandomizedElectionTimeout()) //重置选举定时器计时（随机时间）
	DPrintf("[Node %v][Term %v] from %v to FOLLOWER", rf.me, rf.currentTerm, rf.state)
}

//转为candidate之后，开始选举：
//增加currentTerm
//为自己投票
//重设选举定时器
//给所有的服务器发送RequestVote RPC
func (rf *Raft) becomeCandidate() {
	rf.state = StateCandidate
	rf.StartElection()                                        //重新竞举
	resetTimer(rf.electionTimer, RandomizedElectionTimeout()) //重置选举定时器计时（随机时间）
	DPrintf("[Node %v][Term %v] from %v to CANDIDATE", rf.me, rf.currentTerm, rf.state)
}

func (rf *Raft) IsCandidate() bool {
	return rf.state == StateCandidate
}
func (rf *Raft) IsFollower() bool {
	return rf.state == StateFollower
}
func (rf *Raft) IsLeader() bool {
	return rf.state == StateLeader
}
