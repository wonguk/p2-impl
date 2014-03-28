package storageserver

func leaseMaster(key string, add chan string, revoke, done chan struct{}) {
	leases := make(map[int]chan bool)
	back := make(chan int)
	leaseMap := make(map[string]int)
	cur := 0

	for {
		select {
		// StructureNode tells leaseMaster to revoke leases
		case <-revoke:
			if len(leases) == 0 {
				done <- nil
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
					done <- nil
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
	d := structurerpc.LeaseSeconds + structurerpc.LeaseGuardSeconds

	epoch := time.NewTimer(time.Duration(d) * time.Second)
	for {
		select {
		case <-kill:
			libst, err := rpc.DialHttp("tcp", lease)
			if err != nil {
				//TODO Log
			}
			args := &RevokeLeaseArgs{key}
			var reply RevokeLeaseReply

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
