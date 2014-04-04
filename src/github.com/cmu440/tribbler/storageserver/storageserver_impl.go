package storageserver

import (
	"errors"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/storagerpc"
)

type Nodes []storagerpc.Node

func (n Nodes) Len() int           { return len(n) }
func (n Nodes) Swap(i, j int)      { n[i], n[j] = n[j], n[i] }
func (n Nodes) Less(i, j int) bool { return n[i].NodeID < n[j].NodeID }

type storageServer struct {
	master       bool
	masterLock   sync.Mutex
	masterAddr   string
	numNodes     int
	port         int
	nodeID       uint32
	nodePosition uint32
	servers      Nodes

	ready   chan struct{}
	isReady bool

	nodeHandlers map[string]chan command
	nodeBalChan  chan command
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

	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	hostport := hostname + ":" + strconv.Itoa(port)

	ss.masterLock = sync.Mutex{}
	ss.masterAddr = masterServerHostPort
	ss.numNodes = numNodes
	ss.port = port
	ss.nodeID = nodeID
	ss.servers = Nodes{
		storagerpc.Node{
			HostPort: hostport,
			NodeID:   nodeID,
		},
	}

	ss.ready = make(chan struct{})

	ss.nodeHandlers = make(map[string]chan command)
	ss.nodeBalChan = make(chan command)

	rpc.RegisterName("RemoteStorageServer", ss)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":"+strconv.Itoa(port))
	if e != nil {
		return nil, errors.New("error listening to port" + strconv.Itoa(port))
	}
	go http.Serve(l, nil)

	if ss.master {
		<-ss.ready
	} else {
		client, err := rpc.DialHTTP("tcp", masterServerHostPort)
		if err != nil {
			return nil, err
		}

		args := storagerpc.RegisterArgs{
			ServerInfo: storagerpc.Node{
				HostPort: hostport,
				NodeID:   nodeID,
			},
		}
		var reply storagerpc.RegisterReply

		for {
			err = client.Call("RegisterServer", &args, &reply)

			if err != nil {
				return nil, err
			}

			if reply.Status == storagerpc.OK {
				ss.servers = reply.Servers
				sort.Sort(Nodes(ss.servers))

				for i, node := range ss.servers {
					if node.NodeID == ss.nodeID {
						ss.nodePosition = uint32(i)
						break
					}
				}
				break
			}

			time.Sleep(1 * time.Second)
		}
	}

	ss.isReady = true

	return ss, nil
}

func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	if !ss.master {
		return errors.New("cannot register server to a slave server")
	}

	ss.masterLock.Lock()
	defer ss.masterLock.Unlock()

	if len(ss.servers) == ss.numNodes-1 {
		reply.Status = storagerpc.OK
		reply.Servers = ss.servers
		return nil
	}

	for _, node := range ss.servers {
		if node.NodeID == args.ServerInfo.NodeID {
			reply.Status = storagerpc.NotReady
			return nil
		}
	}

	ss.servers = append(ss.servers, args.ServerInfo)

	if len(ss.servers) == ss.numNodes-1 {
		reply.Status = storagerpc.OK
		reply.Servers = ss.servers
		sort.Sort(Nodes(reply.Servers))

		for i, node := range ss.servers {
			if node.NodeID == ss.nodeID {
				ss.nodePosition = uint32(i)
			}
		}

		close(ss.ready)
	} else {
		reply.Status = storagerpc.NotReady
	}

	return nil
}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	if !ss.isReady {
		reply.Status = storagerpc.NotReady
		return nil
	}

	reply.Status = storagerpc.OK
	reply.Servers = ss.servers

	return nil
}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	if !ss.isReady {
		reply.Status = storagerpc.NotReady
		return nil
	}

	if !ss.isKeyOkay(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	resultChan := make(chan error)
	gc := getCmd{args, reply, resultChan}

	ss.nodeBalChan <- &gc

	return <-resultChan
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	if !ss.isReady {
		reply.Status = storagerpc.NotReady
		return nil
	}

	if !ss.isKeyOkay(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	resultChan := make(chan error)
	glc := getListCmd{args, reply, resultChan}

	ss.nodeBalChan <- &glc

	return <-resultChan
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !ss.isReady {
		reply.Status = storagerpc.NotReady
		return nil
	}

	if !ss.isKeyOkay(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	resultChan := make(chan error)
	pc := putCmd{args, reply, resultChan}

	ss.nodeBalChan <- &pc

	return <-resultChan
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !ss.isReady {
		reply.Status = storagerpc.NotReady
		return nil
	}

	if !ss.isKeyOkay(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	resultChan := make(chan error)
	ac := appendCmd{args, reply, resultChan}

	ss.nodeBalChan <- &ac

	return <-resultChan
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !ss.isReady {
		reply.Status = storagerpc.NotReady
		return nil
	}

	if !ss.isKeyOkay(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	resultChan := make(chan error)
	rc := removeCmd{args, reply, resultChan}

	ss.nodeBalChan <- &rc

	return <-resultChan
}

//For a given cmd, redirects it to the relevant channel
func (ss *storageServer) nodeBalancer() {
	for {
		select {
		case c := <-ss.nodeBalChan:
			key := c.getKey()
			nodeChan, ok := ss.nodeHandlers[key]

			if ok {
				nodeChan <- c
			} else {
				nc := c.init()
				if nc != nil {
					ss.nodeHandlers[key] = nc
				}
			}
		}
	}
}

// Checks whether the given key is at the correct server
func (ss *storageServer) isKeyOkay(key string) bool {
	hash := libstore.StoreHash(strings.Split(key, ":")[0])

	for _, node := range ss.servers {
		if node.NodeID >= hash {
			return ss.nodeID == node.NodeID
		}
	}

	return ss.nodeID == ss.servers[0].NodeID
}
