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
	servers      []storagerpc.Node

	ready   chan struct{}
	isReady bool

	nodeHandlers map[string]chan *cmd
	nodeBalChan  chan *cmd
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
