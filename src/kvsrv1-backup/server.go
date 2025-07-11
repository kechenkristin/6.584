package kvsrv

import (
	"log"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Reply struct {
	Value   string
	Version rpc.Tversion
	Err     rpc.Err
}

type Operation struct {
	Key       string
	Value     string
	Version   rpc.Tversion
	IsPut     bool
	ReplyChan chan Reply
}

type KVServer struct {
	kvStore map[string]struct {
		Value   string
		Version rpc.Tversion
	}
	requests chan Operation
}

func (kv *KVServer) guardian() {
	for op := range kv.requests {
		if op.IsPut {
			DPrintf("Guardian: Received Put for key '%s', value '%s', version %d", op.Key, op.Value, op.Version)
		} else {
			DPrintf("Guardian: Received Get for key '%s'", op.Key)
		}

		var reply Reply

		if op.IsPut {
			item, exists := kv.kvStore[op.Key]
			if exists {
				if op.Version == item.Version {
					kv.kvStore[op.Key] = struct {
						Value   string
						Version rpc.Tversion
					}{op.Value, item.Version + 1}
					reply = Reply{Value: op.Value, Version: kv.kvStore[op.Key].Version, Err: rpc.OK}
				} else {
					reply = Reply{Err: rpc.ErrVersion}
				}
			} else {
				if op.Version == 0 {
					kv.kvStore[op.Key] = struct {
						Value   string
						Version rpc.Tversion
					}{op.Value, 1}
					reply = Reply{Value: op.Value, Version: 1, Err: rpc.OK}
				} else {
					reply = Reply{Err: rpc.ErrNoKey}
				}
			}
		} else {
			item, exists := kv.kvStore[op.Key]
			if exists {
				DPrintf("Guardian: Found key '%s'! Value in store is '%s', version %d", op.Key, item.Value, item.Version)
				reply = Reply{Value: item.Value, Version: item.Version, Err: rpc.OK}
			} else {
				reply = Reply{Err: rpc.ErrNoKey}
			}
		}
		DPrintf("Guardian: Replying to op for key '%s'. Value: '%s', Version: %d, Err: '%v'", op.Key, reply.Value, reply.Version, reply.Err)
		op.ReplyChan <- reply
	}
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		kvStore: make(map[string]struct {
			Value   string
			Version rpc.Tversion
		}),
		requests: make(chan Operation, 100),
	}
	go kv.guardian()
	return kv
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	myReplyChan := make(chan Reply)
	op := Operation{
		Key:       args.Key,
		IsPut:     false,
		ReplyChan: myReplyChan,
	}
	kv.requests <- op
	res := <-myReplyChan
	reply.Value = res.Value
	reply.Version = res.Version
	reply.Err = res.Err
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	myReplyChan := make(chan Reply)
	op := Operation{
		Key:       args.Key,
		Value:     args.Value,
		Version:   args.Version,
		IsPut:     true,
		ReplyChan: myReplyChan,
	}
	kv.requests <- op
	res := <-myReplyChan
	reply.Err = res.Err
}

func (kv *KVServer) Kill() {}

func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
