package storageserver

import (
	"sync"

	"github.com/cmu440/tribbler/rpc/storagerpc"
)

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
	LOGV.Println("GetCmd:", c.args.Key)
	c.reply.Status = storagerpc.OK
	c.reply.Value = node.data

	// Lease
	if c.args.WantLease {
		leaseReq := make(leaseRequest)
		node.leaseRequest <- leaseReq

		if <-leaseReq {
			LOGV.Println("GetCmd:", "Granting lease to", c.args.Key)
			node.addLease <- c.args.HostPort
			c.reply.Lease.Granted = true
			c.reply.Lease.ValidSeconds = storagerpc.LeaseSeconds
		} else {
			LOGV.Println("GetCmd:", "Node currently in use", c.args.Key)
			c.reply.Lease.Granted = false
		}
	}

	c.result <- nil
}

func (c *getCmd) init() chan command {
	LOGE.Println("GetCmd:", "Key not found", c.args.Key)
	c.reply.Status = storagerpc.KeyNotFound
	c.result <- nil

	return nil
}

func (c *getCmd) getKey() string {
	return c.args.Key
}

func (c *getListCmd) run(node *storageNode) {
	LOGV.Println("GetListCmd:", c.args.Key)
	c.reply.Status = storagerpc.OK
	c.reply.Value = node.listData

	// Lease
	if c.args.WantLease {
		LOGV.Println("GetListCmd:", "Granting lease to", c.args.Key)
		leaseReq := make(leaseRequest)
		node.leaseRequest <- leaseReq

		if <-leaseReq {
			node.addLease <- c.args.HostPort
			c.reply.Lease.Granted = true
			c.reply.Lease.ValidSeconds = storagerpc.LeaseSeconds
		} else {
			c.reply.Lease.Granted = false
		}
	}

	c.result <- nil
}

func (c *getListCmd) init() chan command {
	LOGE.Println("GetListCmd:", "Key not found", c.args.Key)
	c.reply.Status = storagerpc.KeyNotFound
	c.result <- nil

	return nil
}

func (c *getListCmd) getKey() string {
	return c.args.Key
}

func (c *putCmd) run(node *storageNode) {
	LOGV.Println("PutCmd:", c.args.Key)
	node.putMutex.Lock()
	defer node.putMutex.Unlock()

	LOGV.Println("PutCmd:", "revoking leases...", c.args.Key)
	node.revokeLease <- true
	<-node.doneLease
	LOGV.Println("PutCmd:", "leases revoked", c.args.Key)

	node.data = c.args.Value
	node.releaseLease <- true

	c.reply.Status = storagerpc.OK
	c.result <- nil
}

func (c *putCmd) init() chan command {
	LOGV.Println("PutCmd:", "Initializing storage node...", c.args.Key)
	sn := new(storageNode)

	sn.data = c.args.Value
	sn.addLease = make(chan string)
	sn.revokeLease = make(chan bool)
	sn.releaseLease = make(chan bool)
	sn.leaseRequest = make(chan leaseRequest)
	sn.doneLease = make(chan bool)
	sn.commands = make(chan command)
	sn.putMutex = new(sync.Mutex)

	go sn.handleNode()
	go leaseMaster(c.args.Key, sn.addLease, sn.revokeLease, sn.doneLease, sn.releaseLease, sn.leaseRequest)

	c.reply.Status = storagerpc.OK
	c.result <- nil

	return sn.commands
}

func (c *putCmd) getKey() string {
	return c.args.Key
}

func (c *appendCmd) run(node *storageNode) {
	LOGV.Println("AppendCmd:", c.args.Key)
	node.putMutex.Lock()
	defer node.putMutex.Unlock()
	for _, d := range node.listData {
		if d == c.args.Value {
			LOGE.Println("AppendCmd:", c.args.Key, c.args.Value, "Item Exists!")
			c.reply.Status = storagerpc.ItemExists
			c.result <- nil
			return
		}
	}

	LOGV.Println("AppendCmd:", "revoking leases...", c.args.Key)
	node.revokeLease <- true
	<-node.doneLease
	LOGV.Println("AppendCmd:", "leases revoked", c.args.Key)

	node.listData = append(node.listData, c.args.Value)
	node.releaseLease <- true

	c.reply.Status = storagerpc.OK
	c.result <- nil
}

func (c *appendCmd) init() chan command {
	LOGV.Println("AppendCmd:", "Initializing storage node...", c.args.Key)
	sn := new(storageNode)

	sn.listData = []string{c.args.Value}
	sn.addLease = make(chan string)
	sn.revokeLease = make(chan bool)
	sn.releaseLease = make(chan bool)
	sn.leaseRequest = make(chan leaseRequest)
	sn.doneLease = make(chan bool)
	sn.commands = make(chan command)
	sn.putMutex = new(sync.Mutex)

	go sn.handleNode()
	go leaseMaster(c.args.Key, sn.addLease, sn.revokeLease, sn.doneLease, sn.releaseLease, sn.leaseRequest)

	c.reply.Status = storagerpc.OK
	c.result <- nil

	return sn.commands
}

func (c *appendCmd) getKey() string {
	return c.args.Key
}

func (c *removeCmd) run(node *storageNode) {
	LOGV.Println("RemoveCmd:", c.args.Key)
	node.putMutex.Lock()
	defer node.putMutex.Unlock()

	LOGV.Println("RemoveCmd:", "Looking for", c.args.Value, c.args.Key)
	for i := range node.listData {
		if node.listData[i] == c.args.Value {
			LOGV.Println("RemoveCmd:", "Found Value!", c.args.Key)

			LOGV.Println("RemoveCmd:", "revoking leases...", c.args.Key)
			node.revokeLease <- true
			<-node.doneLease
			LOGV.Println("RemoveCmd:", "leases revoked", c.args.Key)

			node.listData = append(node.listData[:i], node.listData[i+1:]...)

			node.releaseLease <- true

			c.reply.Status = storagerpc.OK
			c.result <- nil

			return
		}
	}

	LOGE.Println("RemoveCmd:", "Item Not Found")
	c.reply.Status = storagerpc.ItemNotFound
	c.result <- nil
}

func (c *removeCmd) init() chan command {
	LOGE.Println("RemoveCmd:", "Key Not Found")
	c.reply.Status = storagerpc.KeyNotFound
	c.result <- nil

	return nil
}

func (c *removeCmd) getKey() string {
	return c.args.Key
}
