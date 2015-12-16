package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "bytes"
import "time"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Port int
	Typ  string
	Args interface{}
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.
	kvdb      map[string]string
	lastrpcid map[int64]int
	ops       []Op
	curSeq    int
}

/**
**需要注意的是，server之间需要用paxos协议来保持一致
**/
func Encode(data interface{}) []byte {
	buf := bytes.NewBuffer(nil)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(data)
	//fmt.Println("data:", data)

	if err != nil {
		fmt.Println("err:", err)
		return nil
	}
	return buf.Bytes()
}
func Decode(data []byte, to interface{}) error {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	return dec.Decode(to)
}

//触发日志在startpaxos中大概比较好，在每次start后都读1次
//append可能有问题，因此如果paxo发现此值已经commit了，直接退出
func (kv *KVPaxos) waitForStatus(cur int) interface{} {
	to := 10 * time.Millisecond
	decided, curTV := kv.px.Status(cur)
	for decided != paxos.Decided {
		decided, curTV = kv.px.Status(cur)
		time.Sleep(to)
		/*if to < time.Second {
			to *= 2
		}*/
		//fmt.Println("to:", to)
	}
	return curTV
}
func (kv *KVPaxos) StartPaxos(cur int, res Op, port int) {

	for i := 0; i < cur; i++ {
		decided, curTV := kv.px.Status(i)
		if decided != paxos.Decided {
			continue
		}
		curV := Op{}
		Decode(Encode(curTV), &curV)
		if curV.Args == res.Args {
			//fmt.Println("peidui!!!!!")
			return
		}
	}
	kv.px.Start(cur, res)
	curTV := kv.waitForStatus(cur)
	var curV Op
	Decode(Encode(curTV), &curV)
	for curV.Port != port {
		cur++
		//fmt.Println("cur:", cur, port, curV.Port, curV, curTV)
		kv.px.Start(cur, res)
		curTV := kv.waitForStatus(cur)
		curV = Op{}
		Decode(Encode(curTV), &curV)
		//fmt.Println("afterDecode:", cur, port, curV.Port, curV, curTV)
	}
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	anop := Op{kv.me, args.Op, *args}
	cur := kv.px.Max() + 1
	kv.StartPaxos(cur, anop, anop.Port)
	//kv.mu.Lock()
	//defer kv.mu.Unlock()
	maxSeq := kv.px.Max()
	//t1, t2 := kv.px.Status(maxSeq)
	for ; kv.curSeq <= maxSeq; kv.curSeq++ {
		//curTStatus := kv.waitForStatus(kv.curSeq)
		_, curTStatus := kv.px.Status(kv.curSeq)
		var curStatus Op
		//fmt.Println("get:", curTStatus, kv.curSeq, maxSeq)
		if curTStatus == nil {
			continue
		}
		Decode(Encode(curTStatus), &curStatus)
		var paargs PutAppendArgs
		if curStatus.Typ == "Get" {
			continue
		}
		Decode(Encode(curStatus.Args), &paargs)
		t1, ok1 := kv.lastrpcid[paargs.Clientid]
		if ok1 && t1 >= paargs.RpcId {
			continue
		}
		kv.lastrpcid[paargs.Clientid] = paargs.RpcId
		kv.PutAppendImp(&paargs, &PutAppendReply{})
	}
	if v, ok := kv.kvdb[args.Key]; ok {
		reply.Value = v
		reply.Err = OK
	} else {
		reply.Err = ErrNoKey
		reply.Value = ""
	}
	//fmt.Println("maxSeq:", maxSeq)
	//kv.px.Done(maxSeq) //加了这一个就出错，是因为，之前还没有放到磁盘，就被删除掉了
	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	anop := Op{kv.me, args.Op, *args}
	cur := kv.px.Max() + 1
	kv.StartPaxos(cur, anop, anop.Port)
	reply.Err = OK
	return nil
}
func (kv *KVPaxos) PutAppendImp(args *PutAppendArgs, reply *PutAppendReply) error {
	//kv.mu.Lock()
	//defer kv.mu.Unlock()
	if args.Op == "Put" {
		kv.kvdb[args.Key] = args.Value
		reply.Err = OK
	} else if args.Op == "Append" {
		reply.Err = OK
		if v, ok := kv.kvdb[args.Key]; ok {
			kv.kvdb[args.Key] = v + args.Value
		} else {
			kv.kvdb[args.Key] = args.Value
		}
	}
	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})
	gob.Register(PutAppendArgs{})
	gob.Register(GetArgs{})
	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.ops = make([]Op, 0)
	kv.curSeq = 0
	kv.kvdb = make(map[string]string)
	kv.lastrpcid = make(map[int64]int)
	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
