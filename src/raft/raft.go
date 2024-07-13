package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Command interface{}) (index, Term, isleader)
//   start agreement on a new Log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the Log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"bytes"
	"log"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive Log Entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed Log entry.
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

// log的标准格式就是一个command加一个term
type Log struct {
	Command interface{}
	Term    int
}

func (l *Log) copy() *Log {
	return &Log{
		Command: l.Command,
		Term:    l.Term,
	}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers. 	不变, 不需要mu保护.
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[] 不变, 不需要mu保护.
	dead      int32               // set by Kill()
	identity  int32

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	persistedState *PersistedState
	leaderState    *LeaderState

	commitIndex int // 最新一次被commit的log的index. 初始化为0.只在任期内有意义
	lastApplied int // 最新一次被实际执行的log的index. 初始化为0.只在任期内有意义

	// 从这里开始, 不需要mu保护
	heartBeat *HeartBeat
	applyCh   chan ApplyMsg
}

const (
	HEARTBEAT_INTERVAL     = 150 * time.Millisecond
	TIMEOUT_MULT_FACTOR_LB = 2
	TIMEOUT_MULT_FACTOR_UB = 6

	IDENTITY_FOLLOWER int32 = iota
	IDENTITY_CANDIDATE
	IDENTITY_LEADER
)

func getRandomizedTimeout() time.Duration {
	timeout := (TIMEOUT_MULT_FACTOR_LB + rand.Float64()*(TIMEOUT_MULT_FACTOR_UB-TIMEOUT_MULT_FACTOR_LB)) * float64(HEARTBEAT_INTERVAL)
	return time.Duration(timeout)
}

// 用来承接从persister中拿出的数据
type PersistedState struct {
	CurrentTerm int   // 节点所看到的最新的任期. 初始化为0
	VotedFor    int   // 当前任期内接收到vote的candidateId
	Log         []Log // log中除了command外, 还包含接收到command时的term
}

func NewPersistedState() *PersistedState {
	log := make([]Log, 0)
	return &PersistedState{
		CurrentTerm: 0,
		VotedFor:    -1, // -1表示null
		Log:         log,
	}
}

type LeaderState struct {
	nextIndex  []int // 每个server下次要发送的log的index. 只限leader. 初始化为leader last Log index + 1.
	matchIndex []int // 每个server上已知的, 最新被执行的log的index. 初始化为0. 只限leader.
}

func NewLeaderState(rf *Raft) *LeaderState {
	nextIndexInit := len(rf.persistedState.Log) + 1
	nextIndex := make([]int, len(rf.peers))
	for i := 0; i < len(nextIndex); i++ {
		nextIndex[i] = nextIndexInit
	}
	return &LeaderState{
		nextIndex:  nextIndex,
		matchIndex: make([]int, len(rf.peers)),
	}
}

func (l *LeaderState) decreaseNextIndex(server int) {
	l.nextIndex[server]--
}

func (l *LeaderState) increaseNextIndex(server int) {
	l.nextIndex[server]++
}

type HeartBeat struct {
	mu       sync.Mutex
	received bool
}

func (h *HeartBeat) hasReceivedAndReset() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.received {
		h.received = false
		return true
	}
	return false
}

func (h *HeartBeat) receive() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.received = true
}

func (rf *Raft) GetIdentity() int32 {
	return atomic.LoadInt32(&rf.identity)
}

func (rf *Raft) SetIdentity(newIdentity int32) {
	atomic.StoreInt32(&rf.identity, newIdentity)
}

// return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persistedState.CurrentTerm, rf.GetIdentity() == IDENTITY_LEADER
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(rf.persistedState)
	if err != nil {
		return
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the Log through (and including)
// that index. Raft should now trim its Log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// Raft系统的一大规则: rpc的被调用者拿到请求后, 以及rpc的调用者收到回复后, 都要检查消息中的term.
// 如果peer给出的term大于自身了, 就需要增大自身的term以及相关的属性, 且明确自身是follower.
// 这个方法可以无脑放到rpc方法的最开始, 以及读取rpc reply的方法中
func (rf *Raft) updateIfNewTerm(term int) {
	if term > rf.persistedState.CurrentTerm {
		rf.persistedState.CurrentTerm = term
		rf.persistedState.VotedFor = -1
		rf.leaderState = nil
		rf.SetIdentity(IDENTITY_FOLLOWER)
		rf.persist()
		//log.Printf("rf %d updated to term %d\n", rf.me, term)
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
// 整个方法是上锁的, 因此多次requestVote会顺序执行.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.updateIfNewTerm(args.Term) // don't change

	// 对于来自落后term的candidate, 直接拒绝
	if args.Term < rf.persistedState.CurrentTerm {
		reply.VoteGranted = false
		reply.Term = rf.persistedState.CurrentTerm
		log.Printf("rf %d as a follower rejected vote request from candidate %d due to outdated term\n", rf.me, args.CandidateId)
		return
	}

	reply.Term = args.Term
	// 如果当前term已经投过票且投的不是candidate, 返回false. 不能重新投
	if rf.persistedState.VotedFor != -1 && rf.persistedState.VotedFor != args.CandidateId {
		log.Printf("rf %d as a follower rejected vote request from candidate %d due to already voted in this term", rf.me, args.CandidateId)
		reply.VoteGranted = false
		return
	}

	// 如果candidate的log还不如自己的新, 返回false.
	logLength := len(rf.persistedState.Log)
	var lastLog *Log = nil
	if logLength > 0 {
		lastLog = &rf.persistedState.Log[logLength-1]
	}
	if lastLog != nil && (args.LastLogTerm < lastLog.Term || (args.LastLogTerm == lastLog.Term && args.LastLogIndex < logLength)) {
		log.Printf("rf %d as a follower rejected vote request from candidate %d due to outdated log", rf.me, args.CandidateId)
		reply.VoteGranted = false
		return
	}
	// grant vote时认为收到heart beat
	rf.heartBeat.receive()

	log.Printf("rf %d as a follower approved vote request from candidate %d\n", rf.me, args.CandidateId)
	rf.persistedState.VotedFor = args.CandidateId
	rf.persist()
	rf.SetIdentity(IDENTITY_FOLLOWER)
	reply.VoteGranted = true
	rf.heartBeat.receive() // 只有成功vote时, 才认为得到leader心跳
	return
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
	log.Printf("rf %d has received request vote result from %d\n", rf.me, server)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      *Log
	LeaderCommit int
}

func (a *AppendEntriesArgs) copy() *AppendEntriesArgs {
	return &AppendEntriesArgs{
		Term:         a.Term,
		LeaderId:     a.LeaderId,
		PrevLogIndex: a.PrevLogIndex,
		PrevLogTerm:  a.PrevLogTerm,
		Entries:      a.Entries,
		LeaderCommit: a.LeaderCommit,
	}
}

// 专门用来发送最新收到的log的
func NewAppendEntriesArg(rf *Raft, currentLogIndex int) *AppendEntriesArgs {
	var prevLogIndex, prevLogTerm int
	prevLogIndex = currentLogIndex - 1
	prevLogArrIdx := prevLogIndex - 1
	if prevLogArrIdx < len(rf.persistedState.Log) && prevLogArrIdx >= 0 {
		prevLogTerm = rf.persistedState.Log[prevLogArrIdx].Term
	} else {
		prevLogTerm = 0
	}

	return &AppendEntriesArgs{
		Term:         rf.persistedState.CurrentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      rf.persistedState.Log[currentLogIndex-1].copy(),
		LeaderCommit: rf.commitIndex,
	}
}

// 专用于给某个server发送失败时进行recover的
func NewServerSpecificAppendEntryArg(rf *Raft, server int) *AppendEntriesArgs {
	var prevLogIndex, prevLogTerm int
	prevLogIndex = rf.leaderState.nextIndex[server] - 1

	if prevLogIndex-1 < len(rf.persistedState.Log) && prevLogIndex >= 1 {
		prevLogTerm = rf.persistedState.Log[prevLogIndex-1].Term
	} else {
		prevLogTerm = rf.persistedState.CurrentTerm
	}

	return &AppendEntriesArgs{
		Term:         rf.persistedState.CurrentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      rf.persistedState.Log[prevLogIndex].copy(),
		LeaderCommit: rf.commitIndex,
	}
}

func NewHeartBeatArg(rf *Raft, server int) *AppendEntriesArgs {
	var prevLogIndex, prevLogTerm int
	prevLogIndex = rf.leaderState.nextIndex[server] - 1

	if prevLogIndex-1 < len(rf.persistedState.Log) && prevLogIndex >= 1 {
		prevLogTerm = rf.persistedState.Log[prevLogIndex-1].Term
	} else {
		prevLogTerm = rf.persistedState.CurrentTerm
	}

	return &AppendEntriesArgs{
		Term:         rf.persistedState.CurrentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      nil,
		LeaderCommit: rf.commitIndex,
	}
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//log.Printf("rf %d received heartbeat from leader %d\n", rf.me, args.LeaderId)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// arg term < 当前term时, 拒绝 (且不认为这是一条合理消息)
	if args.Term < rf.persistedState.CurrentTerm {
		reply.Success = false
		reply.Term = rf.persistedState.CurrentTerm
		return
	}

	argPrevLogArrIdx := args.PrevLogIndex - 1
	// idx超出了边界, 无法append
	if argPrevLogArrIdx >= len(rf.persistedState.Log) || argPrevLogArrIdx < -1 {
		reply.Success = false
		reply.Term = rf.persistedState.CurrentTerm
		return
	}

	// term 检查, 判断prevLog的term是否匹配
	// arrIdx = -1时, 说明这是第一条log, 不需要检查prev log的term,
	if argPrevLogArrIdx > -1 {
		// term检查
		myPrevLog := rf.persistedState.Log[argPrevLogArrIdx]
		if myPrevLog.Term != args.PrevLogTerm {
			reply.Success = false
			reply.Term = rf.persistedState.CurrentTerm
			return
		}
	}

	// 完成检查, 到这里认为是合理请求
	rf.heartBeat.receive()
	rf.updateIfNewTerm(args.Term)

	argCurrentLogIndex, argCurrentLogArrIdx := args.PrevLogIndex+1, argPrevLogArrIdx+1

	// argPrevLogArrIdx < len(log)  =>  argCurrentLogArrIdx <= len(log)

	// 还需要检查, current log和arg incoming log有无冲突
	// 存在current log
	if argCurrentLogArrIdx < len(rf.persistedState.Log) {
		myCurrentLog := rf.persistedState.Log[argCurrentLogArrIdx]
		if myCurrentLog.Term != args.Term {
			// 发生冲突时, 删除myCurrentLog及所有后续, 然后加上新的
			rf.persistedState.Log = rf.persistedState.Log[:argCurrentLogArrIdx]
			log.Printf("rf %d removed conflicted log with index %d\n", rf.me, myCurrentLog)
			if args.Entries != nil {
				rf.persistedState.Log = append(rf.persistedState.Log, *args.Entries)
				log.Printf("rf %d persisted log with index %d\n", rf.me, argCurrentLogIndex)
			}
		} else {
			// 不需要append了
		}
	} else {
		// 不存在current log, 说明argCurrentLogArrIdx == len(log), 直接append
		if args.Entries != nil {
			rf.persistedState.Log = append(rf.persistedState.Log, *args.Entries)
		}
	}

	rf.persist()

	rf.commitUntilTargetIfApplicable(args.LeaderCommit)

	reply.Success = true
	reply.Term = rf.persistedState.CurrentTerm
	return
}

func (rf *Raft) commitUntilTargetIfApplicable(targetIndex int) {
	if targetIndex > rf.commitIndex {
		var ub int
		if targetIndex <= len(rf.persistedState.Log) {
			ub = targetIndex
		} else {
			ub = len(rf.persistedState.Log)
		}
		log.Printf("rf %d: leader commmit is %d, my commit is %d, commit ub is %d\n", rf.me, targetIndex, rf.commitIndex, ub)
		//fmt.Println(rf.persistedState.Log)
		for i := rf.commitIndex + 1; i <= ub; i++ {
			commandIndex := i
			command := rf.persistedState.Log[commandIndex-1].Command
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      command,
				CommandIndex: commandIndex,
			}
		}
		rf.commitIndex = ub
	}
}

func (rf *Raft) sendAppendEntriesAsHeartBeat(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if rf.killed() {
		return false
	}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok && reply != nil {
		rf.mu.Lock()
		rf.updateIfNewTerm(reply.Term)
		rf.mu.Unlock()
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, counter *int32) {
	if rf.killed() {
		return
	}
	// 循环的目的是让传入的arg里的log entry被persist成功. 不过, 循环可能会修改args.
	// 最后, 原本的arg entry被persist时, arg的prevLogIndex会回到和原本的prevLogIndex一致.
	targetPrevIndex := args.PrevLogIndex
	for !rf.killed() {
		// 每一轮都要更新arg和reply的地址!
		roundArgs := args.copy()
		roundReply := &AppendEntriesReply{}
		received := int32(0)
		// 不能依赖laprpc自身的timeout, 因为他给的timeout太长了!
		go func() {
			log.Printf("rf %d as leader send rf %d command Index %d\n", rf.me, server, roundArgs.PrevLogIndex+1)
			ok := rf.peers[server].Call("Raft.AppendEntries", roundArgs, roundReply)
			if ok {
				atomic.StoreInt32(&received, 1)
			}
		}()
		time.Sleep(100 * time.Millisecond)
		if 1 != atomic.LoadInt32(&received) || roundReply == nil {
			// avoid overly frequent call
			log.Printf("rf %d failed to respond the call with command Index %d\n", server, args.PrevLogIndex+1)
			rf.mu.Lock()
			args.Term = rf.persistedState.CurrentTerm
			args.LeaderCommit = rf.commitIndex
			rf.mu.Unlock()
			continue
		}

		// result is ok and we have reply
		rf.mu.Lock()
		rf.updateIfNewTerm(roundReply.Term)
		rf.mu.Unlock()
		// may not be leader anymore
		if rf.GetIdentity() != IDENTITY_LEADER {
			break
		}

		if !roundReply.Success {
			rf.mu.Lock()
			// decrease the index, change the log to be sent, repopulate the arg
			rf.leaderState.decreaseNextIndex(server)
			log.Printf("rf %d failed to acknowledge the entry. now set it next index to %d\n", server, rf.leaderState.nextIndex[server])
			args = NewServerSpecificAppendEntryArg(rf, server)
			rf.mu.Unlock()
			continue
		}

		rf.mu.Lock()
		rf.leaderState.increaseNextIndex(server)
		if args.PrevLogIndex == targetPrevIndex {
			// add counter to let the caller know this server has persisted the log
			atomic.AddInt32(counter, 1)
			rf.mu.Unlock()
			log.Printf("rf %d as leader received follower %d success persistence message for log %d \n", rf.me, server, args.PrevLogIndex+1)
			break
		} else {
			// there are still some log left to be sync. re populate the log and continue the loop.
			args = NewServerSpecificAppendEntryArg(rf, server)
			log.Printf("resume rf %d and send with prevLog %d\n", server, args.PrevLogIndex)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) SendHeartBeat() {
	for !rf.killed() {
		if IDENTITY_LEADER == rf.GetIdentity() {
			rf.mu.Lock()
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				args := NewHeartBeatArg(rf, i)
				//log.Printf("rf leader %d is sending heart beat to rf %d \n", rf.me, i)
				go rf.sendAppendEntriesAsHeartBeat(i, args, &AppendEntriesReply{})
			}
			rf.mu.Unlock()
		}
		time.Sleep(HEARTBEAT_INTERVAL)
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's Log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft Log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the Command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	// Your code here (2B).
	isLeader := rf.GetIdentity() == IDENTITY_LEADER && !rf.killed()
	if !isLeader {
		return index, term, false
	}

	// Leader接受到消息后, 首先将command persist到log中
	rf.mu.Lock()
	rf.persistedState.Log = append(rf.persistedState.Log, Log{
		Command: command,
		Term:    rf.persistedState.CurrentTerm,
	})
	rf.persist()

	commandIndex := len(rf.persistedState.Log)
	log.Printf("rf %d as leader persisted command index %d\n", rf.me, commandIndex)

	// 然后并发发起appendEntries来将command送给其他peers
	// 如果follower宕机/不响应, 无限重试, 直到所有follower最终存住全部log (不过, 这一步不会阻塞当前的start方法)

	// 用counter来记录append entry 结果
	var counter int32 = 1
	majority := int32((len(rf.peers) + 1) / 2)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		args := NewAppendEntriesArg(rf, commandIndex)
		log.Printf("rf %d as leader appending entries to rf %d with log index %d \n", rf.me, i, args.PrevLogIndex+1)
		go rf.sendAppendEntries(i, args, &AppendEntriesReply{}, &counter)
	}
	rf.mu.Unlock()

	/**
	"当entry被安全地复制后"(或者说, committed), 正式让state machine执行这一command (通过applyCh). 等待state machine执行结束后, 将结果返回.
	committed的定义是, 在多数机器上, log被成功persist
	因此, 这里在并发发送完请求后, 真正阻塞的条件是: 收到多数机器的persist成功消息
	*/

	// 等待多数机器persist成功
	for rf.GetIdentity() == IDENTITY_LEADER && atomic.LoadInt32(&counter) < majority {
		time.Sleep(10 * time.Millisecond)
		//log.Printf("rf %d as leader waiting for appending entries. persisted: %d, majority: %d\n", rf.me, atomic.LoadInt32(&counter), majority)
	}
	rf.mu.Lock()
	rf.commitIndex++
	defer rf.mu.Unlock()
	//log.Printf("rf %d as leader commited command %s\n", rf.me, command)

	// 交给state machine执行
	rf.applyCh <- ApplyMsg{
		CommandValid: true,
		Command:      command,
		CommandIndex: commandIndex,
	}

	// 确认到log被commit后, leader需要维护更新其已知的最新被commit的index (volatile. commitIndex)
	// 后续的appendEntries(包括心跳)会发出这个index. Follower收到后, 确认log被commit, 就可以apply到local的state machine

	return rf.commitIndex, rf.persistedState.CurrentTerm, true
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
	log.Printf("rf %d was killed\n", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) initElection() bool {
	rf.mu.Lock()

	// 发起选举.
	//log.Printf("rf %d didn't receive heart beat and is initiating an election with term %d\n", rf.me, rf.persistedState.CurrentTerm+1)
	rf.SetIdentity(IDENTITY_CANDIDATE)

	// 先准备好rpc消息 并且增大current Term
	rf.persistedState.CurrentTerm += 1
	rf.persistedState.VotedFor = rf.me
	rf.persist()

	currentTerm := rf.persistedState.CurrentTerm
	lastLogIndex := len(rf.persistedState.Log)
	var lastLogTerm int
	if lastLogIndex == 0 {
		lastLogTerm = rf.persistedState.CurrentTerm
	} else {
		lastLogTerm = rf.persistedState.Log[lastLogIndex-1].Term
	}
	rf.mu.Unlock()

	var vote int32
	atomic.StoreInt32(&vote, 1)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		args := &RequestVoteArgs{
			Term:         currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}

		reply := &RequestVoteReply{}
		//log.Printf("rf %d send request vote to rf %d\n", rf.me, i)
		server := i
		go func() {
			ok := rf.sendRequestVote(server, args, reply)
			if ok {
				if reply.VoteGranted {
					atomic.AddInt32(&vote, 1)
				} else {
					rf.mu.Lock()
					rf.updateIfNewTerm(reply.Term)
					rf.mu.Unlock()
				}
			}
		}()
	}

	// 只能通过sleep一段时间来等待选举的反馈. 时间不宜太长, 也不应该随机化.
	//  (或者: 通过channel监听requestVote的结果, 在vote成功后/全部回复后/超时后退出监听)
	// 当然, 时间也不能太短. 至少应该有1, 2个rpc的ttl.
	// 理想化来说, 这个时间应该小于另一个candidate苏醒(未发生) - 当前candidate苏醒(已发生)的时间差.
	// 这个时间差的期望是(upper bound - lower bound) / server 数量
	time.Sleep(HEARTBEAT_INTERVAL)
	//log.Printf("rf %d has received %d vote\n", rf.me, vote)

	// 如果已经是follower, 代表已经有leader/更优秀的candidate出现, 那么直接退出选举, 继续监听心跳
	if rf.GetIdentity() == IDENTITY_FOLLOWER {
		return true
	}

	// 否则, 若收到半数以上选票, 说明竞选成功, 直接成为leader
	if int(atomic.LoadInt32(&vote)) >= (len(rf.peers)+1)/2 {
		log.Printf("rf %d received majority vote and convert to leader on term %d\n", rf.me, rf.persistedState.CurrentTerm)
		rf.makeLeader()
		// todo: 也许需要在选举成功后直接发送appendEntries
		return true
	}

	// 否则, 说明竞选失败, 也没有其他成功者, 返回false, 继续下一轮选举
	//log.Printf("rf %d failed the election on term%d\n", rf.me, rf.persistedState.CurrentTerm)
	return false
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {

	// kill变量本身是原子性的, 所以这里调用rf.killed()不用担心race的问题
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		// testcase要求leader的心跳间隔大于 100 ms
		// 选举超时的下限设定为 max(心跳间隔, 选举耗时). 还要考虑各节点选举超时的时间差和选举耗时的关系. 因此上限应该较大.来拉开时间差.
		// 暂定leader心跳间隔为150 ms. 因此选举超时下限取心跳的3倍. 上限为心跳的10倍. (考虑到需要多轮投票来选举).

		//log.Printf("rf %d has an election timeout of %d ms\n", rf.me, int(timeout/float64(time.Millisecond)))
		time.Sleep(getRandomizedTimeout())

		// wake up 后可能状态已经改变了. 所以再检查一次
		if rf.killed() {
			break
		}

		// 超时时间达到后, 检查过去时间内是否收到心跳. 只有follower需要检查.
		// heart beat自身带锁, 不必特地上锁
		if IDENTITY_FOLLOWER != rf.GetIdentity() {
			//log.Printf("rf %d is leader, no need for heart beat\n", rf.me)
			continue
		}

		if rf.heartBeat.hasReceivedAndReset() {
			//log.Printf("rf %d has received heartbeat\n", rf.me)
			continue
		}

		// 发起选举. 如果成功则直接退出. 否则继续选举
		for !rf.initElection() {
		}
	}
}

func (rf *Raft) makeLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	nextIndex := make([]int, len(rf.peers))
	idx := len(rf.persistedState.Log) + 1
	for i := range nextIndex {
		nextIndex[i] = idx
	}
	rf.leaderState = NewLeaderState(rf)
	rf.SetIdentity(IDENTITY_LEADER)

	log.Println("🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉")
	log.Printf("rf %d has became the leader\n", rf.me)
	log.Println("🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉🎉")
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
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.persistedState = NewPersistedState()
	rf.heartBeat = &HeartBeat{
		mu:       sync.Mutex{},
		received: false,
	}
	rf.SetIdentity(IDENTITY_FOLLOWER)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.SendHeartBeat()
	//log.Printf("rf %d (id: %d)started\n", me, rf.id)
	return rf
}

// for utility only
func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
}
