package storageserver

import (
	"net/rpc"
	"time"

	"github.com/cmu440/tribbler/rpc/storagerpc"
)

func leaseMaster(key string, add chan string, revoke, done chan bool) {
	LOGV.Println("Lease Master:", key, "Starting lease master for")
	leases := make(map[int]chan bool)
	back := make(chan int)
	leaseMap := make(map[string]int)
	cur := 0

	for {
		select {
		// StructureNode tells leaseMaster to revoke leases
		case <-revoke:
			LOGV.Println("Lease Master:", key, "revoking leases for")

			if len(leases) == 0 {
				done <- true
				break
			}

			// Notify all the leases to revoke
			for _, r := range leases {
				r <- false
			}

			// Delete leases one by one and return when none left
			for {
				LOGV.Println("Lease Master:", key, len(leases), "leases left")
				l := <-back
				LOGV.Println("Lease Master:", key, "Lease revoked by", l)
				delete(leases, l)

				if len(leases) == 0 {
					LOGV.Println("Lease Master:", key, "Done revoking lease")
					done <- true
					break
				}
			}

		// StructureNode has added a new lease
		case l := <-add:
			LOGV.Println("Lease Master:", key, "Lease requested by", l)
			n, ok := leaseMap[l]

			if ok {
				LOGV.Println("Lease Master:", key, "Removing old lease for", l)
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
			LOGV.Println("Lease Master:", key, "removing lease", l)
			delete(leases, l)
		}
	}
}

func leaseHandler(lease, key string, i int, kill chan bool, back chan int) {
	LOGV.Println("Lease Handler:", key, "starting lease", i, "for", lease)
	d := storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds

	epoch := time.NewTimer(time.Duration(d) * time.Second)
	for {
		select {
		case <-kill:
			LOGV.Println("Lease Handler:", key, "Revoking lease")
			libst, err := rpc.DialHTTP("tcp", lease)
			if err != nil {
				LOGE.Println("Lease Handler:", key, "libstore connect error:", err)
			}
			defer libst.Close()

			args := &storagerpc.RevokeLeaseArgs{key}
			var reply storagerpc.RevokeLeaseReply

			err = libst.Call("LeaseCallbacks.RevokeLease", args, &reply)
			if err != nil {
				LOGE.Println("Lease Handler:", key, "revokelease error:", err)
			}

			// TODO Check status?
			back <- i
			return
		case <-epoch.C:
			LOGV.Println("Lease Handler:", key, lease, i, "Lease expired")
			back <- i
			return
		}
	}
}
