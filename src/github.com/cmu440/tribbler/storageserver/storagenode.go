package storageserver

type storageNode struct {
	data        string
	listData    []string
	addLease    chan string
	revokeLease chan bool
	doneLease   chan bool
	commands    chan command
}

func (sn *storageNode) handleNode() {
	for {
		select {
		case c := <-sn.commands:
			c.run(sn)
		}
	}
}
