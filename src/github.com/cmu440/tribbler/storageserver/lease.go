package storageserver

import (
	//	"fmt"
	"net/rpc"
	"time"

	"github.com/cmu440/tribbler/rpc/storagerpc"
)

type leaseRequest chan bool

func leaseMaster(key string, add chan string, revoke, done, release chan bool, leaseReq chan leaseRequest, client chan clientRequest) {
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

			go leaseHandler(l, key, cur, kill, back, client)

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

func leaseHandler(lease, key string, i int, kill chan bool, back chan int, client chan clientRequest) {
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
			req := clientRequest{lease, make(chan *rpc.Client)}
			client <- req

			libst := <-req.ret
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

type clientRequest struct {
	lib string
	ret chan *rpc.Client
}

type libstoreClients struct {
	clients map[string]*rpc.Client
	reqChan chan clientRequest
}

func (l *libstoreClients) handleClients() {
	for {
		//fmt.Println("BBBBBBBBBBB", len(l.reqChan))
		select {
		case req := <-l.reqChan:
			client, ok := l.clients[req.lib]

			if ok {
				req.ret <- client
			} else {
				libst, err := rpc.DialHTTP("tcp", req.lib)
				if err != nil {
					LOGE.Println("ASDASD")
				}

				l.clients[req.lib] = libst
				req.ret <- libst
			}
		}
	}
}
