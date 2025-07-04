package kvsrv

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
	"time"
)

type Clerk struct {
	clnt   *tester.Clnt
	server string
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server}
	return ck
}

// In client.go
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := &rpc.GetArgs{
		Key: key,
	}

	for { // Loop forever until we get a definitive reply
		reply := &rpc.GetReply{}
		ok := ck.clnt.Call(ck.server, "KVServer.Get", args, &reply)

		if ok {
			// The server responded! Its reply is the truth.
			// The logic here is simple: if the server says OK, we return OK.
			// If it says ErrNoKey, we return ErrNoKey.
			// We don't need to check the specific error types here, just pass it through.
			// Also, a successful Get should never return ErrVersion.
			return reply.Value, reply.Version, reply.Err
		}
		
		// The RPC call failed. Wait a bit and let the loop retry.
		time.Sleep(100 * time.Millisecond)
	}
}

// In client.go
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	args := &rpc.PutArgs{
		Key:     key,
		Value:   value,
		Version: version,
	}
	
	// This flag is our "memory".
	firstAttempt := true

	for { // Loop forever until we get a definitive reply
		reply := &rpc.PutReply{}
		ok := ck.clnt.Call(ck.server, "KVServer.Put", args, &reply)

		if ok {
			// The server responded.
			if reply.Err == rpc.ErrVersion {
				// The server rejected the request due to a version mismatch.
				// Now we must be honest about our certainty.
				if firstAttempt {
					// This was our first and only try. The operation definitely failed.
					return rpc.ErrVersion
				} else {
					// This was a retry after a network failure. We don't know if the
					// first attempt succeeded. We must report uncertainty.
					return rpc.ErrMaybe
				}
			}
			// For any other reply (like OK or ErrNoKey), the result is certain.
			return reply.Err
		}

		// The RPC call failed. The next attempt will be a retry.
		firstAttempt = false
		time.Sleep(100 * time.Millisecond)
	}
}