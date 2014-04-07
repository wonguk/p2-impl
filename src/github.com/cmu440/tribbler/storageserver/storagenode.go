package storageserver

import "sync"

type storageNode struct {
	data         string
	listData     []string
	addLease     chan string
	revokeLease  chan bool
	releaseLease chan bool
	leaseRequest chan leaseRequest
	doneLease    chan bool
	commands     chan command
	putMutex     *sync.Mutex
}

func (sn *storageNode) handleNode() {
	for {
		select {
		case c := <-sn.commands:
			LOGV.Println("StorageNode:", "Recieved Command!")
			go c.run(sn)
		}
	}
}
