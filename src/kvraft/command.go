package kvraft

import (
	"6.824/labgob"
	"bytes"
	"fmt"
	"time"
)

//Command
//server层 ->直接提交到raft层
//channel ⬆️ notify通知server层状态已经应用，可以返回结果给客户端了
//状态机层 异步applier协程
//raft层 ⬆️ applyCh激活状态机层应用状态
type Command struct {
	*CommandRequest
}

const ExecuteTimeout = 500 * time.Millisecond

//Command 服务端统一处理命令逻辑函数
func (kv *KVServer) Command(request *CommandRequest, response *CommandResponse) {
	defer DPrintf("[Command]<Node %v|Term %v> processes Command {op %v|Id %v} with Response {val %v|err %v}", kv.rf.Me(), kv.rf.Term(), request.Op, request.CommandId, response.Value, response.Err)
	// return result directly without raft layer's participation if request is duplicated
	kv.mu.RLock()
	//读请求由于不改变系统的状态，重复执行多次是没问题的。
	if request.Op != OpGet && kv.isDuplicateRequest(request.ClientId, request.CommandId) {
		lastResponse := kv.lastOperations[request.ClientId].LastResponse //如果重复，用该操作的上一次操作结果直接拦截返回
		response.Value, response.Err = lastResponse.Value, lastResponse.Err
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()
	//不要持有锁来提高吞吐量
	//当KVServer持有锁以获取快照时，底层raft仍然可以提交raft日志
	index, _, isLeader := kv.rf.Start(Command{request})
	if !isLeader { //只有leader才能执行命令
		response.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	ch := kv.getNotifyChan(index) //监听applier的返回，收到后回复客户端，并且有超时
	kv.mu.Unlock()

	//客户端协程阻塞等待结果时超时返回：为了避免客户端发送到少数派 leader 后被长时间阻塞，其在交给 leader 处理后阻塞时需要考虑超时，一旦超时就返回客户端让其重试。
	select {
	case result := <-ch:
		DPrintf("[Command]<Node %v|Term %v>received result {val %v|op %v|err %v} from applier goroutine", kv.rf.Me(), kv.rf.Term(), result.Value, result.Op.String(), result.Err)
		response.Value, response.Err = result.Value, result.Err
	case <-time.After(ExecuteTimeout):
		DPrintf("[Command]<Node %v|Term %v>wait applier timeout", kv.rf.Me(), kv.rf.Term())
		response.Err = ErrTimeout
	}
	//释放notifyChan以减少内存占用
	//为什么异步？为了提高吞吐量，这里不需要阻止客户端请求
	go func() {
		kv.mu.Lock()
		kv.removeOutdatedNotifyChan(index)
		kv.mu.Unlock()
	}()
}

//applier a dedicated applier goroutine to apply committed entries to stateMachine, take snapshot and apply snapshot from raft
func (kv *KVServer) applier() {
	for kv.killed() == false {
		select {
		case message := <-kv.applyCh:
			DPrintf("[applier]<Node %v|Term %v> tries to apply message %v", kv.rf.Me(), kv.rf.Term(), message)
			if message.CommandValid {
				kv.mu.Lock()
				if message.CommandIndex <= kv.lastApplied { //raft层尝试应用的commandId比server已经应用的commandId小于等于，过期消息
					DPrintf("[applier]<Node %v|Term %v> discards outdated message %v because a newer snapshot which lastApplied is %v has been restored", kv.rf.Me(), kv.rf.Term(), message, kv.lastApplied)
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = message.CommandIndex

				var response *CommandResponse
				command := message.Command.(Command)
				//这里由判断了一次duplicate，第一次是服务端收到命令时，第二次是服务端状态机层收到raft层传过来的消息时
				//第一次粗拦截，因为必须保证只应用到状态机一次，这一次是根本保证
				if command.Op != OpGet && kv.isDuplicateRequest(command.ClientId, command.CommandId) {
					DPrintf("{Node %v} doesn't apply duplicated message %v to stateMachine because maxAppliedCommandId is %v for client %v", kv.rf.Me(), message, kv.lastOperations[command.ClientId], command.ClientId)
					response = kv.lastOperations[command.ClientId].LastResponse
				} else {
					response = kv.applyLogToStateMachine(command)
					DPrintf("[applier]<Term %v|%v>apply command {Id %v|op %v|key %v|val %v} response{val %v|err %v}", kv.rf.Term(), kv.rf.Me(), command.CommandId, command.Op.String(), command.Key, command.Value, response.Value, response.Err)
					if command.Op != OpGet {
						kv.lastOperations[command.ClientId] = OperationContext{command.CommandId, response}
					}
				}

				// 仅对 leader 的 notifyChan 进行通知：目前的实现中读写请求都需要路由给 leader 去处理，
				//所以在执行日志到状态机后，只有 leader 需将执行结果通过 notifyChan 唤醒阻塞的客户端协程，而 follower 则不需要；
				//对于 leader 降级为 follower 的情况，该节点在 apply 日志后也不能对之前靠 index 标识的 channel 进行 notify，
				//因为可能执行结果是不对应的，所以在加上只有 leader 可以 notify 的判断后，
				//对于此刻还阻塞在该节点的客户端协程，就可以让其自动超时重试。如果读者足够细心，也会发现这里的机制依然可能会有问题，下一点会提到。
				//仅对当前 term 日志的 notifyChan 进行通知：
				//上一点提到，对于 leader 降级为 follower 的情况，该节点需要让阻塞的请求超时重试以避免违反线性一致性。
				//那么有没有这种可能呢？leader 降级为 follower 后又迅速重新当选了 leader，而此时依然有客户端协程未超时在阻塞等待，
				//那么此时 apply 日志后，根据 index 获得 channel 并向其中 push 执行结果就可能出错，因为可能并不对应。
				//对于这种情况，最直观地解决方案就是仅对当前 term 日志的 notifyChan 进行通知，让之前 term 的客户端协程都超时重试即可。
				//当然，目前客户端超时重试的时间是 500ms，选举超时的时间是 1s，所以严格来说并不会出现这种情况，
				//但是为了代码的鲁棒性，最好还是这么做，否则以后万一有人将客户端超时时间改到 5s 就可能出现这种问题了。
				if currentTerm, isLeader := kv.rf.GetState(); isLeader && message.CommandTerm == currentTerm {
					ch := kv.getNotifyChan(message.CommandIndex)
					ch <- response
				}

				if kv.needSnapshot() { //log超过设定的大小maxraftstate
					DPrintf("[applier]<Node %v|Term %v> need snapShot", kv.rf.Me(), kv.rf.Term())
					kv.takeSnapshot(message.CommandIndex)
				}
				DPrintf("[applier]<Node %v|Term %v> logSize: %v", kv.rf.Me(), kv.rf.Term(), kv.rf.GetRaftStateSize())
				kv.mu.Unlock()
			} else if message.SnapshotValid { //快照需要应用到状态机
				//apply 协程负责持锁阻塞式的去生成 snapshot，幸运的是，此时 raft 框架是不阻塞的，依然可以同步并提交日志，只是不 apply 而已。
				//如果这里还想进一步优化的话，可以将状态机搞成 MVCC 等能够 COW 的机制，这样应该就可以不阻塞状态机的更新了。
				kv.mu.Lock()
				//快照apply消息，调用cond是为了把raft层日志以及状态位重置下，返回false说明快照消息是落后的，不用搭理
				if kv.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
					kv.restoreSnapshot(message.Snapshot) //真正应用快照到状态机
					kv.lastApplied = message.SnapshotIndex
				}
				kv.mu.Unlock()
			} else {
				panic(fmt.Sprintf("unexpected Message %v", message))
			}
		}
	}
}

//getNotifyChan lock before use
func (kv *KVServer) getNotifyChan(commandIndex int) chan *CommandResponse {
	if _, ok := kv.notifyChans[commandIndex]; !ok {
		//这里一定要用有缓冲的
		kv.notifyChans[commandIndex] = make(chan *CommandResponse, 1)
	}
	return kv.notifyChans[commandIndex]
}

//needSnapshot lock before use
func (kv *KVServer) needSnapshot() bool {
	if kv.maxraftstate == -1 {
		return false
	}

	return kv.rf.GetRaftStateSize() >= kv.maxraftstate
}

//takeSnapshot 日志的 snapshot 不仅需要包含状态机的状态，还需要包含用来去重的 lastOperations 哈希表。
//lock before use
func (kv *KVServer) takeSnapshot(commandIndex int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.stateMachine)
	e.Encode(kv.lastOperations)
	e.Encode(kv.lastApplied)

	DPrintf("[takeSnapshot]<Node %v|Term %v> snapShot: %v", kv.me, kv.rf.Term(), w.Bytes())

	//生成快照，本地log直接清空
	kv.rf.Snapshot(commandIndex, w.Bytes())
}

//restoreSnapshot lock before use
func (kv *KVServer) restoreSnapshot(snapshot []byte) {
	if snapshot != nil {
		var (
			stateMachine   MemoryKV
			lastOperations map[int64]OperationContext
			lastApplied    int
		)
		r := bytes.NewBuffer(snapshot)
		d := labgob.NewDecoder(r)

		if d.Decode(&stateMachine) != nil ||
			d.Decode(&lastOperations) != nil ||
			d.Decode(&lastApplied) != nil {
			DPrintf("[restoreSnapshot]<Node %v|Term %v> fails to recover from snapshot", kv.me, kv.rf.Term())
		} else {
			kv.stateMachine = stateMachine
			kv.lastOperations = lastOperations
			kv.lastApplied = lastApplied
		}
	}
}

//applyLogToStateMachine lock before use
func (kv *KVServer) applyLogToStateMachine(command Command) *CommandResponse {
	var (
		value string
		err   Err
		op    = command.Op
	)
	switch op {
	case OpGet:
		value, err = kv.stateMachine.Get(command.Key)
	case OpPut:
		err = kv.stateMachine.Put(command.Key, command.Value)
	case OpAppend:
		err = kv.stateMachine.Append(command.Key, command.Value)
	}

	return &CommandResponse{
		Value: value,
		Err:   err,
		Op:    op,
	}
}

//removeOutdatedNotifyChan lock before use
func (kv *KVServer) removeOutdatedNotifyChan(index int) {
	delete(kv.notifyChans, index)
}
