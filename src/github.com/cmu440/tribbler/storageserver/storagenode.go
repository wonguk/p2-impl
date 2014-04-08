package storageserver

import (
	"container/list"
	//"fmt"
	"sync"
)

type storageNode struct {
	data string
	//listData     []string
	listData     *list.List
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
		//fmt.Println("CCCCCCCCCC", len(sn.commands))
		select {
		case c := <-sn.commands:
			LOGV.Println("StorageNode:", "Recieved Command!")
			go c.run(sn)
		}
	}
}
