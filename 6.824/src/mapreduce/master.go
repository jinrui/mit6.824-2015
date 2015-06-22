package mapreduce

import "container/list"
import "fmt"

type WorkerInfo struct {
	address string
	// You can add definitions here.
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	go func() {
		for mr.alive {
			worker := <-mr.registerChannel
			workerInfo := new(WorkerInfo)
			workerInfo.address = worker
			mr.Workers[worker] = workerInfo
			mr.WorkersState <- worker
		}
	}()
	for i := 0; i < mr.nMap; i = i + <-mr.mrLock { //mybe deadLock
		go func() {
			args := &DoJobArgs{}
			args.File = mr.file
			args.Operation = Map
			args.NumOtherPhase = mr.nReduce
			var reply DoJobReply
			args.JobNumber = i
			mr.mrLock <- 1
			for ok := false; ok == false; {
				key := <-mr.WorkersState
				ok = call(key, "Worker.DoJob", args, &reply)
				mr.WorkersState <- key //this  is deadlock, it may block when no other thread wait for worker
			}
			mr.AllWorkDone <- true
		}()
	}

	for i := 0; i < mr.nMap; i++ {
		<-mr.AllWorkDone
	}

	for i := 0; i < mr.nReduce; i = i + <-mr.mrLock {
		go func() {
			args := &DoJobArgs{}
			args.File = mr.file
			args.Operation = Reduce
			args.NumOtherPhase = mr.nMap
			var reply DoJobReply
			args.JobNumber = i
			mr.mrLock <- 1
			for ok := false; ok == false; {
				key := <-mr.WorkersState
				ok = call(key, "Worker.DoJob", args, &reply)
				mr.WorkersState <- key
			}
			mr.AllWorkDone <- true
		}()

	}

	for i := 0; i < mr.nReduce; i++ {
		<-mr.AllWorkDone
	}
	//TODO
	return mr.KillWorkers()
}
