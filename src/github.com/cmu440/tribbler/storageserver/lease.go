package storageserver

import (
	"net/rpc"
	"time"

	"github.com/cmu440/tribbler/rpc/storagerpc"
)

type leaseRequest chan bool

func leaseMaster(key string, add chan string, revoke, done, release chan bool, leaseReq chan leaseRequest) {
	LOGV.Println("Lease Master:", key, "Starting lease master for")
	leases := make(map[int]chan bool)
	back := make(chan int)
	leaseMap := make(map[string]int)
	cur := 0
	leaseAvail := true

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

			leaseAvail = false

			// Delete leases one by one and return when none left
			/*for {
				LOGV.Println("Lease Master:", key, len(leases), "leases left")
				l := <-back
				LOGV.Println("Lease Master:", key, "Lease revoked by", l)
				delete(leases, l)

				if len(leases) == 0 {
					LOGV.Println("Lease Master:", key, "Done revoking lease")
					done <- true
					break
				}
			}*/

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

			if !leaseAvail && len(leases) == 0 {
				LOGV.Println("Lease Master:", key, "Done Revoking Lease")
				done <- true
			}
		case req := <-leaseReq:
			req <- leaseAvail
		case <-release:
			leaseAvail = true
		}
	}
}

func leaseHandler(lease, key string, i int, kill chan bool, back chan int) {
	LOGV.Println("Lease Handler:", key, "starting lease", i, "for", lease)

	d := storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds
	epoch := time.NewTimer(time.Duration(d) * time.Second)

	args := new(storagerpc.RevokeLeaseArgs)
	reply := new(storagerpc.RevokeLeaseReply)
	args.Key = key

	revoke := new(rpc.Call)

	for {
		select {
		case <-kill:
			LOGV.Println("Lease Handler:", key, "Revoking lease")
			libst, err := rpc.DialHTTP("tcp", lease)
			if err != nil {
				LOGE.Println("Lease Handler:", key, lease, "libstore connect error:", err)
			}
			defer libst.Close()

			revoke = libst.Go("LeaseCallbacks.RevokeLease", args, reply, nil)

			// TODO Check status?
		case <-epoch.C:
			LOGV.Println("Lease Handler:", key, lease, i, "Lease expired")
			back <- i
			return
		case <-revoke.Done:
			LOGV.Println("Lease Handler:", key, lease, i, "Lease Revoked")
			back <- i
			return

		}
	}
}
