package storageserver

import "github.com/cmu440/tribbler/rpc/storagerpc"

type cmd interface {
	run(node *storageNode) error
}

type getCmd struct {
	args   *storagerpc.GetArgs
	reply  *storagerpc.GetReply
	result chan error
}

type getListCmd struct {
	args   *storagerpc.GetArgs
	reply  *sotragerpc.GetListReply
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

func (c *getCmd) run(node *storageNode) error {
	c.reply.Status = storagerpc.Ok
	c.reply.Value = node.data

	//TODO Lease
	if c.args.WantLease {
		node.addLease <- c.args.HostPort
		c.reply.Lease.Granted = true
		c.reply.Lease.ValidSeconds = storagerpc.LeaseSeconds
	}

	return nil
}

func (c *getListCmd) run(node *storageNode) error {
	c.reply.Status = sotragerpc.Ok
	c.reply.Value = node.listData

	//TODO Lease
	if c.args.WantLease {
		node.addLease <- c.args.HostPort
		c.reply.Lease.Granted = true
		c.reply.Lease.ValidSeconds = storagerpc.LeaseSeconds
	}

	return nil
}

func (c *putCmd) run(node *storageNode) error {
	node.revokeLease <- nil
	<-node.doneLease

	node.data = c.args.Value
	c.reply.Status = storagerpc.Ok

	return nil
}

func (c *appendCmd) run(node *storageNode) error {
	for d := range node.listData {
		if d == c.args.Value {
			c.reply.Status = storagerpc.ItemExists
			return nil
		}
	}
	//Lease
	node.revokeLease <- nil
	<-node.doneLease

	node.listData = append(node.listData, c.args.Value)
	c.reply.Status = storagerpc.Ok

	return nil
}

func (c *removeCmd) run(node *storageNode) error {
	for i := range node.listData {
		if node.listData[i] == c.args.Value {
			//Lease
			node.revokeLease <- nil
			<-node.doneLease

			node.listData = append(node.listData[:i], node.listData[i+1:]...)

			c.reply.Status = storagerpc.Ok
			return nil
		}
	}

	c.reply.Status = storagerpc.ItemNotFound
	return nil
}
