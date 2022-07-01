package kvraft

import (
	"fmt"
	"time"
)

//server层 ->直接提交到raft层
//channel ⬆️ notify通知server层状态已经应用，可以返回结果给客户端了
//状态机层 异步applier协程
//raft层 ⬆️ applyCh激活状态机层应用状态
type Command struct {
	*CommandRequest
}

const ExecuteTimeout = 0

//Command 服务端统一处理命令逻辑函数
func (kv *KVServer) Command(request *CommandRequest, response *CommandResponse) {
	defer DPrintf("{Node %v} processes CommandRequest %v with CommandResponse %v", kv.rf.Me(), request, response)
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
		response.Value, response.Err = result.Value, result.Err
	case <-time.After(ExecuteTimeout):
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

// a dedicated applier goroutine to apply committed entries to stateMachine, take snapshot and apply snapshot from raft
func (kv *KVServer) applier() {
	for kv.killed() == false {
		select {
		//raft层消息同步到server层
		case message := <-kv.applyCh:
			DPrintf("{Node %v} tries to apply message %v", kv.rf.Me(), message)
			if message.CommandValid {
				kv.mu.Lock()
				if message.CommandIndex <= kv.lastApplied { //raft层尝试应用的commandId比server已经应用的commandId还小，过期消息
					DPrintf("{Node %v} discards outdated message %v because a newer snapshot which lastApplied is %v has been restored", kv.rf.Me(), message, kv.lastApplied)
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
					if command.Op != OpGet {
						kv.lastOperations[command.ClientId] = OperationContext{command.CommandId, response}
					}
				}

				// only notify related channel for currentTerm's log when node is leader
				if currentTerm, isLeader := kv.rf.GetState(); isLeader && message.CommandTerm == currentTerm {
					ch := kv.getNotifyChan(message.CommandIndex)
					ch <- response
				}

				needSnapshot := kv.needSnapshot()
				if needSnapshot {
					kv.takeSnapshot(message.CommandIndex)
				}
				kv.mu.Unlock()
			} else if message.SnapshotValid {
				kv.mu.Lock()
				if kv.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
					kv.restoreSnapshot(message.Snapshot)
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
		kv.notifyChans[commandIndex] = make(chan *CommandResponse)
	}
	return kv.notifyChans[commandIndex]
}
func (kv *KVServer) needSnapshot() bool {

}
func (kv *KVServer) takeSnapshot(commandIndex int) {

}
func (kv *KVServer) restoreSnapshot(snapshot []byte) {

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
