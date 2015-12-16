package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"
import "time"

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type Status struct {
	Sta Fate
	N_p int
	N_a int
	V   interface{}
}

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// Your data here.
	maxIns  int
	minIns  int
	doneSeq int
	status  map[int]*Status
}

//only a args
type Args struct {
	Name string
	Pid  int
	Seq  int
	V    interface{}
}

//only a reply
type NV struct {
	Succ bool
	Pid  int
	Aid  int
	Seq  int
	V    interface{}
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()
	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//here is my function
func (px *Paxos) checkStatus(seq int, v interface{}) {
	px.mu.Lock()
	defer px.mu.Unlock()
	_, ok := px.status[seq]
	if !ok {
		px.status[seq] = &Status{Pending, -1, -1, nil}
	}
}
func (px *Paxos) Prepare(args *Args, reply *NV) {
	if args.Name == px.peers[px.me] {
		px.mu.Lock()
		defer px.mu.Unlock()
		if args.Pid <= px.status[args.Seq].N_p {
			reply.Succ = false
		} else {
			reply.Succ = true
			reply.V = px.status[args.Seq].V
			px.status[args.Seq].N_p = args.Pid
		}
		reply.Pid = px.status[args.Seq].N_p
	} else {
		call(args.Name, "Paxos.PrepareSer", args, reply)
	}
}
func (px *Paxos) PrepareSer(args *Args, reply *NV) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	_, ok := px.status[args.Seq]
	if !ok {
		px.status[args.Seq] = &Status{Pending, -1, -1, nil}
	}
	if args.Pid <= px.status[args.Seq].N_p {
		reply.Succ = false
	} else {
		reply.Succ = true
		reply.V = px.status[args.Seq].V
		px.status[args.Seq].N_p = args.Pid
	}
	reply.Pid = px.status[args.Seq].N_p
	reply.Aid = px.status[args.Seq].N_a
	return nil
}
func (px *Paxos) Accept(args *Args, reply *NV) {
	if args.Name == px.peers[px.me] {
		px.mu.Lock()
		defer px.mu.Unlock()
		if args.Pid < px.status[args.Seq].N_p {
			reply.Succ = false
		} else {
			reply.Succ = true
			px.status[args.Seq].N_p = args.Pid
			px.status[args.Seq].N_a = args.Pid
			px.status[args.Seq].V = args.V
		}
		reply.Pid = px.status[args.Seq].N_p
	} else {
		call(args.Name, "Paxos.AcceptSer", args, reply)
	}
}
func (px *Paxos) AcceptSer(args *Args, reply *NV) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	_, ok := px.status[args.Seq]
	//fmt.Println("args.Seq:", px.status[args.Seq], args.Name)
	if !ok {
		px.status[args.Seq] = &Status{Pending, -1, -1, nil}
	}
	if args.Pid < px.status[args.Seq].N_p {
		reply.Succ = false
	} else {
		reply.Succ = true
		px.status[args.Seq].N_p = args.Pid
		px.status[args.Seq].N_a = args.Pid
		px.status[args.Seq].V = args.V
		reply.V = args.V
	}
	reply.Pid = px.status[args.Seq].N_p
	return nil
}
func (px *Paxos) Decide(args *Args, reply *NV) {
	if args.Name == px.peers[px.me] {
		px.mu.Lock()
		defer px.mu.Unlock()
		if args.Seq < px.minIns {
			return
		}
		if px.maxIns < args.Seq {
			px.maxIns = args.Seq
		}
		px.status[args.Seq].Sta = Decided
		px.status[args.Seq].V = args.V
	} else {
		call(args.Name, "Paxos.DecideSer", args, reply)
	}
}
func (px *Paxos) DecideSer(args *Args, reply *NV) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	if args.Seq < px.minIns {
		return nil
	}
	_, ok := px.status[args.Seq]
	if !ok {
		px.status[args.Seq] = &Status{Pending, -1, -1, nil}
	}
	px.status[args.Seq].Sta = Decided
	px.status[args.Seq].V = args.V
	if px.maxIns < args.Seq {
		px.maxIns = args.Seq
	}
	return nil
}
func (px *Paxos) Instance(seq int, pid int, v interface{}) (Fate, int) {
	maxInt := -1
	sucInt := 0
	//failId := 0
	var maxSeq int
	_, ok := px.status[seq]
	px.mu.Lock()
	if ok && px.status[seq].Sta == Decided || px.minIns > seq {
		px.mu.Unlock()
		return Forgotten, -1
	}
	if px.doneSeq > seq {
		px.doneSeq = seq - 1
	}
	px.mu.Unlock()
	for i := 0; i < len(px.peers); i++ {
		var reply NV
		args := &Args{}
		args.Pid = pid
		args.Seq = seq
		args.Name = px.peers[i]
		px.Prepare(args, &reply)
		if reply.Succ {
			sucInt++
		}
		if maxInt < reply.Aid && reply.V != nil {
			v = reply.V
			maxInt = reply.Aid
		}
		if maxSeq < reply.Pid {
			maxSeq = reply.Pid
		}
		//fmt.Println("check:", px.me, maxSeq, sucInt, reply.Pid)
	}
	if sucInt <= len(px.peers)/2 {
		//fmt.Println("pending:", px.me, maxSeq, sucInt)
		return Pending, maxSeq
	}
	//fmt.Println("repart:", px.me, maxSeq, sucInt, v)
	sucInt = 0
	//maxInt = 0
	maxSeq = -1
	for i := 0; i < len(px.peers); i++ {
		var reply NV
		args := &Args{}
		args.Pid = pid
		args.Seq = seq
		args.Name = px.peers[i]
		args.V = v
		reply.Succ = false
		px.Accept(args, &reply)
		if reply.Succ {
			sucInt++
		}
		if maxSeq < reply.Pid {
			maxSeq = reply.Pid
		}
	}
	if sucInt <= len(px.peers)/2 {
		return Pending, maxSeq
	}
	for i := 0; i < len(px.peers); i++ {
		var reply NV
		args := &Args{}
		args.Seq = seq
		args.Name = px.peers[i]
		args.V = v
		px.Decide(args, &reply)
	}
	return Decided, maxSeq
}
func (px *Paxos) getNextPid(maxPid int) int {
	tlen := len(px.peers)
	val := maxPid / tlen
	return (val+1)*tlen + px.me
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	//fmt.Println("maxSeqstart:", px.status[seq], px.me, seq)
	go func() {
		px.checkStatus(seq, v)
		p := px.me
		result, cur := px.Instance(seq, p, v)
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		for !px.isdead() && result != Decided {
			time.Sleep(time.Duration(r.Intn(100)) * time.Millisecond)
			p = px.getNextPid(cur)

			result, cur = px.Instance(seq, p, v)
			if result == Forgotten {
				return
			}
		}
	}()
}

//for done
func (px *Paxos) GetDone(args *Args, reply *NV) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	reply.Seq = px.doneSeq
	px.minIns = reply.Seq + 1
	return nil
}

func (px *Paxos) DecideDone(args *Args, reply *NV) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	for k, _ := range px.status {
		if k <= args.Seq {
			//fmt.Println("delete:", k, px.status[k])
			delete(px.status, k)
		}
	}
	px.minIns = args.Seq + 1
	return nil
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {

	// Your code here.
	if seq < px.minIns {
		return
	}
	if seq > px.maxIns {
		seq = px.maxIns
	}
	if px.doneSeq < seq {
		px.doneSeq = seq
	}
	result := px.doneSeq
	for i := 0; i < len(px.peers); i++ {
		if i == px.me {
			continue
		}
		var reply NV
		args := &Args{}
		args.Seq = seq
		args.Name = px.peers[i]
		succ := call(args.Name, "Paxos.GetDone", args, &reply)

		if !succ {
			return
		}
		if result > reply.Seq {
			result = reply.Seq
		}
	}
	px.mu.Lock()
	if result > px.doneSeq {
		result = px.doneSeq
	}
	for k, _ := range px.status {
		if k <= result {
			delete(px.status, k)
		}
	}
	px.minIns = result + 1
	px.mu.Unlock()
	for i := 0; i < len(px.peers); i++ {
		if i == px.me {
			continue
		}
		var reply NV
		args := &Args{}
		args.Seq = result
		args.Name = px.peers[i]
		call(args.Name, "Paxos.DecideDone", args, &reply)
	}
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	/*if px.minIns > px.maxIns {
		return -1
	}*/
	px.mu.Lock()
	defer px.mu.Unlock()
	return px.maxIns
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	px.mu.Lock()
	defer px.mu.Unlock()
	// You code here.
	return px.minIns
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	var v interface{}
	if seq < px.minIns {
		return Forgotten, v
	}
	_, ok := px.status[seq]
	if !ok {
		//fmt.Println("error?:")
		px.status[seq] = &Status{Pending, -1, -1, v}
		//fmt.Println("error!!:")
	}
	return px.status[seq].Sta, px.status[seq].V
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me
	px.status = make(map[int]*Status)
	// Your initialization code here.
	px.maxIns = -1
	px.minIns = 0
	px.doneSeq = 0
	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
