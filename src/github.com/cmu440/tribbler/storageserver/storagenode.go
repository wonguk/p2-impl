package storageserver

type storageNode struct {
	data        string
	listData    []string
	addLease    chan string
	revokeLease chan struct{}
	doneLease   chan struct{}
	commands    chan *cmd
}

func (sn *storageNode) handleNode() {
	for {
		c := <-sn.commands

		c.result <- c.run
	}
}
