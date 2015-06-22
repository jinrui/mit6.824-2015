package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu          sync.Mutex
	l           net.Listener
	dead        int32 // for testing
	rpccount    int32 // for testing
	me          string
	pingMap     map[string]time.Time
	currentView View
	nextView    View
	canChange   bool
	idleServer  map[string]time.Time
	mutex       chan int
	// Your declarations here.
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	// Your code here.
	vs.mutex <- 1
	vs.pingMap[args.Me] = time.Now()
	if args.Me == vs.currentView.Primary {
		if args.Viewnum == vs.currentView.Viewnum {
			vs.canChange = true
			vs.currentView = vs.nextView
		} else if args.Viewnum == 0 {
			vs.promoteToPrimary()
		} else {
			vs.canChange = false
		}
	} else if vs.currentView.Primary == "" {
		vs.currentView.Primary = args.Me
		vs.currentView.Viewnum = 1
		vs.nextView = vs.currentView
	} else if args.Me == vs.currentView.Backup && args.Viewnum == 0 {
		vs.promoteToBackup()
	} else if vs.currentView.Backup == "" {
		vs.idleServer[args.Me] = vs.pingMap[args.Me]
		vs.promoteToBackup()
	} else if args.Me != vs.currentView.Backup {
		vs.idleServer[args.Me] = vs.pingMap[args.Me]
	}
	reply.View = vs.currentView
	<-vs.mutex
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	reply.View = vs.currentView
	return nil
}

func (vs *ViewServer) promoteToPrimary() bool {
	if !vs.canChange {
		return false
	}
	vs.nextView.Primary = vs.currentView.Backup
	vs.promoteToBackup()
	return true
}

func (vs *ViewServer) promoteToBackup() {
	vs.nextView.Backup = ""
	for key, val := range vs.idleServer {
		if time.Now().Sub(val) < DeadPings*PingInterval && vs.nextView.Backup == "" {
			vs.nextView.Backup = key
			delete(vs.idleServer, key)
		} else if time.Now().Sub(val) >= DeadPings*PingInterval {
			delete(vs.idleServer, key)
		}
	}
	vs.nextView.Viewnum++
	if vs.canChange {
		vs.currentView = vs.nextView
		vs.canChange = false
	}
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.
	vs.mutex <- 1
	for key, val := range vs.pingMap {
		if time.Now().Sub(val) > DeadPings*PingInterval {
			if key == vs.currentView.Primary {
				vs.promoteToPrimary()
			} else if key == vs.currentView.Backup {
				vs.promoteToBackup()
			}
		}
	}
	<-vs.mutex
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.canChange = false
	vs.currentView = View{0, "", ""}
	vs.pingMap = make(map[string]time.Time)
	vs.idleServer = make(map[string]time.Time)
	vs.mutex = make(chan int, 1)
	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
