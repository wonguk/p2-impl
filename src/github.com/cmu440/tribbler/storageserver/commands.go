package storageserver

import "github.com/cmu440/tribbler/rpc/storagerpc"

type command interface {
	run(node *storageNode)
	init() chan command
	getKey() string
}

type getCmd struct {
	args   *storagerpc.GetArgs
	reply  *storagerpc.GetReply
	result chan error
}

type getListCmd struct {
	args   *storagerpc.GetArgs
	reply  *storagerpc.GetListReply
	result chan error
}

type putCmd struct {
	args   *storagerpc.PutArgs
	reply  *storagerpc.PutReply
	result chan error
}

type appendCmd struct {
	args   *storagerpc.PutArgs
	reply  *storagerpc.PutReply
	result chan error
}

type removeCmd struct {
	args   *storagerpc.PutArgs
	reply  *storagerpc.PutReply
	result chan error
}

func (c *getCmd) run(node *storageNode) {
	c.reply.Status = storagerpc.OK
	c.reply.Value = node.data

	// Lease
	if c.args.WantLease {
		node.addLease <- c.args.HostPort
		c.reply.Lease.Granted = true
		c.reply.Lease.ValidSeconds = storagerpc.LeaseSeconds
	}

	c.result <- nil
}

func (c *getCmd) init() chan command {
	c.reply.Status = storagerpc.ItemNotFound
	c.result <- nil

	return nil
}

func (c *getCmd) getKey() string {
	return c.args.Key
}

func (c *getListCmd) run(node *storageNode) {
	c.reply.Status = storagerpc.OK
	c.reply.Value = node.listData

	// Lease
	if c.args.WantLease {
		node.addLease <- c.args.HostPort
		c.reply.Lease.Granted = true
		c.reply.Lease.ValidSeconds = storagerpc.LeaseSeconds
	}

	c.result <- nil
}

func (c *getListCmd) init() chan command {
	c.reply.Status = storagerpc.ItemNotFound
	c.result <- nil

	return nil
}

func (c *getListCmd) getKey() string {
	return c.args.Key
}

func (c *putCmd) run(node *storageNode) {
	node.revokeLease <- true
	<-node.doneLease

	node.data = c.args.Value
	c.reply.Status = storagerpc.OK

	c.result <- nil
}

func (c *putCmd) init() chan command {
	sn := new(storageNode)

	sn.data = c.args.Value
	sn.addLease = make(chan string)
	sn.revokeLease = make(chan bool)
	sn.doneLease = make(chan bool)
	sn.commands = make(chan command)

	go sn.handleNode()
	go leaseMaster(c.args.Key, sn.addLease, sn.revokeLease, sn.doneLease)

	c.reply.Status = storagerpc.OK
	c.result <- nil

	return sn.commands
}

func (c *putCmd) getKey() string {
	return c.args.Key
}

func (c *appendCmd) run(node *storageNode) {
	for _, d := range node.listData {
		if d == c.args.Value {
			c.reply.Status = storagerpc.ItemExists
			c.result <- nil
		}
	}
	//Lease
	node.revokeLease <- true
	<-node.doneLease

	node.listData = append(node.listData, c.args.Value)
	c.reply.Status = storagerpc.OK

	c.result <- nil
}

func (c *appendCmd) init() chan command {
	sn := new(storageNode)

	sn.listData = []string{c.args.Value}
	sn.addLease = make(chan string)
	sn.revokeLease = make(chan bool)
	sn.doneLease = make(chan bool)
	sn.commands = make(chan command)

	go sn.handleNode()
	go leaseMaster(c.args.Key, sn.addLease, sn.revokeLease, sn.doneLease)

	c.reply.Status = storagerpc.OK
	c.result <- nil

	return sn.commands
}

func (c *appendCmd) getKey() string {
	return c.args.Key
}

func (c *removeCmd) run(node *storageNode) {
	for i := range node.listData {
		if node.listData[i] == c.args.Value {
			//Lease
			node.revokeLease <- true
			<-node.doneLease

			node.listData = append(node.listData[:i], node.listData[i+1:]...)

			c.reply.Status = storagerpc.OK
			c.result <- nil
		}
	}

	c.reply.Status = storagerpc.ItemNotFound
	c.result <- nil
}

func (c *removeCmd) init() chan command {
	c.reply.Status = storagerpc.ItemNotFound
	c.result <- nil

	return nil
}

func (c *removeCmd) getKey() string {
	return c.args.Key
}
