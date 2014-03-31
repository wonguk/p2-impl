package storageserver

import (
	"net/rpc"
	"time"

	"github.com/cmu440/tribbler/rpc/storagerpc"
)

func leaseMaster(key string, add chan string, revoke, done chan bool) {
	leases := make(map[int]chan bool)
	back := make(chan int)
	leaseMap := make(map[string]int)
	cur := 0

	for {
		select {
		// StructureNode tells leaseMaster to revoke leases
		case <-revoke:
			if len(leases) == 0 {
				done <- true
				break
			}

			//Notify all the leases to revoke
			for _, r := range leases {
				r <- false
			}

			//Delete leases one by one and return when none left
			for {
				l := <-back
				delete(leases, l)

				if len(leases) == 0 {
					done <- true
					break
				}
			}

			// StructureNode has added a new lease
		case l := <-add:
			n, ok := leaseMap[l]

			if ok {
				kill, ok := leases[n]
				if ok {
					kill <- false
				}
			}

			leaseMap[l] = cur

			kill := make(chan bool)
			leases[cur] = kill

			go leaseHandler(l, key, cur, kill, back)

			cur++
		case l := <-back:
			delete(leases, l)
		}
	}
}

func leaseHandler(lease, key string, i int, kill chan bool, back chan int) {
	d := storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds

	epoch := time.NewTimer(time.Duration(d) * time.Second)
	for {
		select {
		case <-kill:
			libst, err := rpc.DialHTTP("tcp", lease)
			if err != nil {
				//TODO Log
			}
			args := &storagerpc.RevokeLeaseArgs{key}
			var reply storagerpc.RevokeLeaseReply

			err = libst.Call("RemoteLeaqseCallbacks.RevokeLease", args, &reply)
			if err != nil {
				// TODO Log
			}

			// TODO Check status?
			back <- i
			return
		case <-epoch.C:
			back <- i
			return
		}
	}
}
