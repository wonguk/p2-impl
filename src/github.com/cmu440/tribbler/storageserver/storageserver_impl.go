package storageserver

import (
	"errors"

	"github.com/cmu440/tribbler/rpc/storagerpc"
)

type storageServer struct {
	master       bool
	masterAddr   string
	numNodes     int
	port         int
	nodeID       uint32
	nodePosition uint32
	servers      []sotragerpc.Node

	ready   chan struct{}
	isReady bool

	nodeHandlers map[string]chan *cmd
	nodeBalChan  chan *cmd
}

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

// NewStorageServer creates and starts a new StorageServer. masterServerHostPort
// is the master storage server's host:port address. If empty, then this server
// is the master; otherwise, this server is a slave. numNodes is the total number of
// servers in the ring. port is the port number that this server should listen on.
// nodeID is a random, unsigned 32-bit ID identifying this server.
//
// This function should return only once all storage servers have joined the ring,
// and should return a non-nil error if the storage server could not be started.
func NewStorageServer(masterServerHostPort string, numNodes, port int, nodeID uint32) (StorageServer, error) {
	ss := new(storageServer)

	if masterServerHostPort == "" {
		ss.master = true
	} else {
		ss.master = false
	}

	ss.masterAddr = masterServerHostPort
	ss.numNodes = numNodes
	ss.port = port
	ss.nodeID = nodeID
	ss.nodeHandlers = make(map[string]chan *cmd)
	ss.nodeBalChan = make(chan *cmd)

	rpc.RegisterName("StorageServer", ss)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":"+port)
	if e != nil {
		return nil, errors.New("Error Listening to port:", port)
	}
	go http.Serve(l, nil)

	<-ss.ready
	ss.isReady = true

	return nil
}

//TODO Implement RegisterServer
func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	return errors.New("not implemented")
}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	if !ss.isReady {
		reply.Status = storagerpc.NotReady
		return nil
	}

	key := strings.Split(args.Key, ":")[0]

	if !ss.isKeyOkay(key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	reply.Status = storagerpc.Ok
	reply.Servers = ss.servers

	return nil
}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	if !ss.isReady {
		reply.Status = storagerpc.NotReady
		return nil
	}

	key := strings.Split(args.Key, ":")[0]

	if !ss.isKeyOkay(key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	resultChan := make(chan error)
	c := getCmd{args, reply, resultChan}

	ss.nodeBalChan <- c

	return <-resultChan
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	if !ss.isReady {
		reply.Status = storagerpc.NotReady
		return nil
	}

	key := strings.Split(args.Key, ":")[0]

	if !ss.isKeyOkay(key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	resultChan := make(chan error)
	c := getListCmd{args, reply, resultChan}

	ss.nodeBalChan <- c

	return <-resultChan
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !ss.isReady {
		reply.Status = storagerpc.NotReady
		return nil
	}

	key := strings.Split(args.Key, ":")[0]

	if !ss.isKeyOkay(key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	resultChan := make(chan error)
	c := putCmd{args, reply, resultChan}

	ss.nodeBalChan <- c

	return <-resultChan
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !ss.isReady {
		reply.Status = storagerpc.NotReady
		return nil
	}

	key := strings.Split(args.Key, ":")[0]

	if !ss.isKeyOkay(key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	resultChan := make(chan error)
	c := appendCmd{args, reply, resultChan}

	ss.nodeBalChan <- c

	return <-resultChan
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !ss.isReady {
		reply.Status = storagerpc.NotReady
		return nil
	}

	key := strings.Split(args.Key, ":")[0]

	if !ss.isKeyOkay(key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	resultChan := make(chan error)
	c := removeCmd{args, reply, resultChan}

	ss.nodeBalChan <- c

	return <-resultChan
}

//For a given cmd, redirects it to the relevant channel
func (ss *storageServer) nodeBalancer() {
	for {
		select {
		case c := <-ss.nodeBalChan:
			key := c.args.Key
			nodeChan, ok := ss.nodeHandlers[key]

			if !ok && c.args.(type) == *storagerpc.GetArgs {
				c.reply.Status = storagerpc.ItemNotFound
				break
			}

			if !ok {
				sn := new(storageNode)

				sn.data = make(map[string]string)
				sn.listData = make(map[string][]string)
				sn.leases = make([]string)
				sn.commands = make(chan *cmd)

				go sn.handleNode()

				ss.nodeHandlers[key] = sn
				nodeChan = sn
			}

			nodeChan <- c
		}
	}
}

// Checks whether the given key is at the correct server
func (ss *storageServer) isKeyOkay(key string) bool {
	hash = libstore.StoreHash(key)

	return ss.nodePosition == (hash % ss.numNodes)
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

			cur += 1
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
