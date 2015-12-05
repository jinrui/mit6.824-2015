package shardmaster

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

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos
	configs    []Config      // indexed by config num
	gids       map[int64]int //每个gid对应的shard数量
	curSeq     int
}

type Op struct {
	// Your data here.
	Port int
	Typ  int
	Args interface{}
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

func (sm *ShardMaster) StartPaxos(cur int, res Op, port int) {
	to := 10 * time.Millisecond
	//fmt.Println("res:", res)
	//fmt.Println("curstart:", cur, port)
	sm.px.Start(cur, res)
	decided, curTV := sm.px.Status(cur)
	for decided != paxos.Decided {
		decided, curTV = sm.px.Status(cur)
		time.Sleep(to)
	}
	var curV Op
	Decode(Encode(curTV), &curV)
	for port != curV.Port {
		cur = sm.px.Max() + 1
		//fmt.Println("cur:", cur, port, curV.Port, curV, curTV)
		sm.px.Start(cur, res)
		decided, curTV = sm.px.Status(cur)
		for decided != paxos.Decided {
			decided, curTV = sm.px.Status(cur)
			//fmt.Println("decided:", decided, paxos.Decided, curTV, curV)
			time.Sleep(to)
		}
		curV = Op{}
		Decode(Encode(curTV), &curV)
		//fmt.Println("afterDecode:", cur, port, curV.Port, curV, curTV)
	}
}

//一般来说，应该用一致性hash比较好，但是为了编程方便，我就不用了。。
func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	anop := Op{sm.me, 0, *args}
	cur := sm.px.Max() + 1
	//res := Encode(anop)
	/*var tmp Op
	Decode(res, &tmp)
	fmt.Println("tmp:", anop, tmp)*/
	sm.StartPaxos(cur, anop, anop.Port)
	//sm.joinImp(args, reply)
	return nil
}
func (sm *ShardMaster) joinImp(args *JoinArgs, reply *JoinReply) {
	curConfig := sm.configs[len(sm.configs)-1]
	_, ok := curConfig.Groups[args.GID]
	if ok {
		return
	}
	var newShards [NShards]int64
	aveShardNum := NShards / (len(sm.gids) + 1)
	sm.gids[args.GID] = 0
	for i, j := range curConfig.Shards {
		if j == 0 {
			newShards[i] = args.GID
			sm.gids[args.GID]++
		} else if sm.gids[j] > aveShardNum+1 {
			newShards[i] = args.GID
			sm.gids[args.GID]++
			sm.gids[j]--
		} else {
			newShards[i] = j
		}
	}
	for i, j := range newShards {
		if sm.gids[args.GID] >= aveShardNum {
			break
		}
		if sm.gids[j] > aveShardNum {
			newShards[i] = args.GID
			sm.gids[args.GID]++
			sm.gids[j]--
		}
	}
	newGroups := make(map[int64][]string)
	for k, v := range curConfig.Groups {
		newGroups[k] = v
	}
	newGroups[args.GID] = args.Servers
	newConfig := Config{curConfig.Num + 1, newShards, newGroups}
	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.
	//aveNumForGid := gids[args.GID] / (len(gids) - 1)
	sm.mu.Lock()
	defer sm.mu.Unlock()
	anop := Op{sm.me, 1, *args}

	cur := sm.px.Max() + 1
	//res := Encode(anop)
	sm.StartPaxos(cur, anop, anop.Port)
	return nil
}
func (sm *ShardMaster) leaveImp(args *LeaveArgs, reply *LeaveReply) {
	delete(sm.gids, args.GID)
	var newShards [NShards]int64
	cur := 0
	curConfig := sm.configs[len(sm.configs)-1]
	_, ok := curConfig.Groups[args.GID]
	if !ok {
		return
	}
	aveShardNum := NShards / len(sm.gids)
	//golang map循环顺序是随机的。。因此这有个大bug!!
	for gid, _ := range sm.gids {
		for cur < len(curConfig.Shards) && sm.gids[gid] < aveShardNum {
			if curConfig.Shards[cur] == args.GID {
				newShards[cur] = gid
				sm.gids[gid]++
			} else {
				newShards[cur] = curConfig.Shards[cur]
			}
			cur++
		}
	}
	for gid, _ := range sm.gids {
		for cur < len(curConfig.Shards) && sm.gids[gid] <= aveShardNum {
			if curConfig.Shards[cur] == args.GID {
				newShards[cur] = gid
				sm.gids[gid]++
			} else {
				newShards[cur] = curConfig.Shards[cur]
			}
			cur++
		}
	}

	newGroups := make(map[int64][]string)
	for k, v := range curConfig.Groups {
		if k != args.GID {
			newGroups[k] = v
		}
	}
	newConfig := Config{curConfig.Num + 1, newShards, newGroups}
	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	anop := Op{sm.me, 2, *args}
	cur := sm.px.Max() + 1
	//res := Encode(anop)
	sm.StartPaxos(cur, anop, anop.Port)
	return nil
}
func (sm *ShardMaster) moveImp(args *MoveArgs, reply *MoveReply) {
	var newShards [NShards]int64
	curConfig := sm.configs[len(sm.configs)-1]
	for i, j := range curConfig.Shards {
		if i == args.Shard {
			sm.gids[j]--
			newShards[i] = args.GID
			sm.gids[args.GID]++
		} else {
			newShards[i] = j
		}
	}
	newGroups := make(map[int64][]string)
	for k, v := range curConfig.Groups {
		newGroups[k] = v
	}
	newConfig := Config{curConfig.Num + 1, newShards, newGroups}
	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	maxSeq := sm.px.Max()
	for ; sm.curSeq <= maxSeq; sm.curSeq++ {
		_, curTStatus := sm.px.Status(sm.curSeq)
		var curStatus Op
		Decode(Encode(curTStatus), &curStatus)
		if curStatus.Typ == 0 {
			var jargs JoinArgs
			//fmt.Println("curStatus.Args:", curTStatus, curStatus)
			Decode(Encode(curStatus.Args), &jargs)
			sm.joinImp(&jargs, &JoinReply{})
		} else if curStatus.Typ == 1 {
			var largs LeaveArgs
			Decode(Encode(curStatus.Args), &largs)
			sm.leaveImp(&largs, &LeaveReply{})
		} else {
			var margs MoveArgs
			Decode(Encode(curStatus.Args), &margs)
			sm.moveImp(&margs, &MoveReply{})
		}
	}
	if args.Num >= len(sm.configs) || args.Num == -1 {
		reply.Config = sm.configs[len(sm.configs)-1]
	} else {
		reply.Config = sm.configs[args.Num]
	}
	return nil
}

// please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

// call this to find out if the server is dead.
func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}
	sm.gids = make(map[int64]int)
	sm.curSeq = 0

	rpcs := rpc.NewServer()

	gob.Register(Op{})
	gob.Register(JoinArgs{})
	gob.Register(LeaveArgs{})
	gob.Register(MoveArgs{})
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.isdead() == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.isdead() == false {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.isdead() == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
