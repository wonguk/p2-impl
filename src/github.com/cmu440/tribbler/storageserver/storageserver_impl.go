package storageserver

import (
	"errors"
	"io/ioutil"
	"log"
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

//var logfile, _ = os.OpenFile("testlogfile", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)

var LOGE = log.New(ioutil.Discard,
	"ERROR [StorageServer] ",
	log.Lmicroseconds|log.Lshortfile)
var LOGV = log.New(ioutil.Discard,
	"VERBOSE [StorageServer] ",
	log.Lmicroseconds|log.Lshortfile)

type storageServer struct {
	master     bool
	masterLock sync.Mutex
	masterAddr string
	numNodes   int
	port       int
	nodeID     uint32
	servers    Nodes

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
	LOGV.Println("Master:", masterServerHostPort, "Nodes:", numNodes, "Port:", port, "NodeID:", nodeID)
	ss := new(storageServer)

	if masterServerHostPort == "" {
		LOGV.Println("Starting Master....", nodeID, port)
		ss.master = true
	} else {
		LOGV.Println("Starting Slave....", nodeID, port)
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

	go ss.nodeBalancer()

	rpc.RegisterName("StorageServer", ss)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":"+strconv.Itoa(port))
	if e != nil {
		LOGE.Println("Error listening to port", e)
		return nil, errors.New("error listening to port" + strconv.Itoa(port))
	}
	go http.Serve(l, nil)

	if ss.master {
		if ss.numNodes == 1 {
			LOGV.Println("Master:", "Done Initializing")
			ss.isReady = true
			return ss, nil
		} else {
			LOGV.Println("Master:", nodeID, "Waiting for nodes to register")
			<-ss.ready
		}
	} else {
		LOGV.Println("Slave:", nodeID, "Dialing master")
		client, err := rpc.DialHTTP("tcp", masterServerHostPort)

		//Try connecting to master until it works
		for err != nil {
			LOGV.Println("Slave:", nodeID, "failed to connect to master", err)
			client, err = rpc.DialHTTP("tcp", masterServerHostPort)
		}
		defer client.Close()

		args := storagerpc.RegisterArgs{
			ServerInfo: storagerpc.Node{
				HostPort: hostport,
				NodeID:   nodeID,
			},
		}
		var reply storagerpc.RegisterReply

		for {
			LOGV.Println("Slave:", nodeID, "registering to master")
			err = client.Call("StorageServer.RegisterServer", &args, &reply)

			if err != nil {
				LOGE.Println("Slave:", nodeID, "error registering to master", err)
				return nil, err
			}

			if reply.Status == storagerpc.OK {
				LOGV.Println("Slave:", nodeID, "Registered to master!")
				ss.servers = reply.Servers
				sort.Sort(Nodes(ss.servers))

				ss.isReady = true
				return ss, nil
			}

			LOGV.Println("Slave:", nodeID, "sleeping for 1 second")
			time.Sleep(1 * time.Second)
		}
	}

	ss.isReady = true

	return ss, nil
}

func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	if !ss.master {
		LOGE.Println("RegisterServer:", "cannot register server to slave")
		return errors.New("cannot register server to a slave server")
	}

	LOGV.Println("RegisterServer:", "registering", args.ServerInfo.NodeID)
	ss.masterLock.Lock()
	defer ss.masterLock.Unlock()

	if len(ss.servers) == ss.numNodes {
		LOGV.Println("RegisterServer:", "Registered all nodes! replying to",
			args.ServerInfo.NodeID)
		reply.Status = storagerpc.OK
		reply.Servers = ss.servers
		return nil
	}

	LOGV.Println("RegisterServer:", "Checking if", args.ServerInfo.NodeID,
		"is already registered")
	for _, node := range ss.servers {
		if node.NodeID == args.ServerInfo.NodeID {
			LOGV.Println("RegisterServer:", args.ServerInfo.NodeID,
				"already registered")
			reply.Status = storagerpc.NotReady
			return nil
		}
	}

	LOGV.Println("RegisterServer:", "Adding", args.ServerInfo.NodeID)
	ss.servers = append(ss.servers, args.ServerInfo)

	if len(ss.servers) == ss.numNodes {
		LOGV.Println("RegisterServer:", "All nodes have been added!")
		sort.Sort(Nodes(ss.servers))

		reply.Status = storagerpc.OK
		reply.Servers = ss.servers

		close(ss.ready)
	} else {
		LOGV.Println("RegisterServer:", len(ss.servers), "out of",
			ss.numNodes, "have been registered")
		reply.Status = storagerpc.NotReady
	}

	return nil
}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	if !ss.isReady {
		LOGE.Println("GetServers:", ss.nodeID, "server is Not Ready")
		reply.Status = storagerpc.NotReady
		return nil
	}

	LOGV.Println("GetServers:", ss.nodeID, "Returning servers")
	reply.Status = storagerpc.OK
	reply.Servers = ss.servers

	return nil
}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	if !ss.isReady {
		LOGE.Println("Get:", ss.nodeID, "server is not ready")
		reply.Status = storagerpc.NotReady
		return nil
	}

	LOGV.Println("Get:", ss.nodeID, "Recieved Get Request for", args.Key)
	if !ss.isKeyOkay(args.Key) {
		LOGE.Println("Get:", ss.nodeID, "Wrong Server!", args.Key)
		reply.Status = storagerpc.WrongServer
		return nil
	}

	LOGV.Println("Get:", ss.nodeID, "Sending request to node bal", args.Key)
	resultChan := make(chan error)
	gc := getCmd{args, reply, resultChan}

	ss.nodeBalChan <- &gc

	result := <-resultChan

	LOGV.Println("Get:", ss.nodeID, "Done!, returning", args.Key, result)

	return result
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	if !ss.isReady {
		LOGE.Println("GetList:", ss.nodeID, "server is not ready")
		reply.Status = storagerpc.NotReady
		return nil
	}

	LOGV.Println("GetList:", ss.nodeID, "Recieved GetList Request for", args.Key)
	if !ss.isKeyOkay(args.Key) {
		LOGE.Println("GetList:", ss.nodeID, "Wrong Server!", args.Key)
		reply.Status = storagerpc.WrongServer
		return nil
	}

	LOGV.Println("GetList:", ss.nodeID, "Sending request to node bal", args.Key)
	resultChan := make(chan error)
	glc := getListCmd{args, reply, resultChan}

	ss.nodeBalChan <- &glc

	result := <-resultChan

	LOGV.Println("GetList:", ss.nodeID, "Done!, returning", args.Key, result)

	return result
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !ss.isReady {
		LOGE.Println("Put:", ss.nodeID, "server is not ready")
		reply.Status = storagerpc.NotReady
		return nil
	}

	LOGV.Println("Put:", ss.nodeID, "Recieved Put Requst", args.Key, args.Value)
	if !ss.isKeyOkay(args.Key) {
		LOGE.Println("Put:", ss.nodeID, "Wrong Server!", args.Key)
		reply.Status = storagerpc.WrongServer
		return nil
	}

	LOGV.Println("Put:", ss.nodeID, "Sending request to nodebal", args.Key)
	resultChan := make(chan error)
	pc := putCmd{args, reply, resultChan}

	ss.nodeBalChan <- &pc

	result := <-resultChan

	LOGV.Println("Put:", ss.nodeID, "Done!, returning", args.Key, result)

	return result
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !ss.isReady {
		LOGE.Println("AppendToList:", ss.nodeID, "server is not ready")
		reply.Status = storagerpc.NotReady
		return nil
	}

	LOGV.Println("AppendToList:", ss.nodeID, "Recieved append request",
		args.Key, args.Value)
	if !ss.isKeyOkay(args.Key) {
		LOGE.Println("AppendToList:", ss.nodeID, "Wrong Server!", args.Key)
		reply.Status = storagerpc.WrongServer
		return nil
	}

	LOGV.Println("AppendToList:", ss.nodeID, "sending request to nodebal",
		args.Key)
	resultChan := make(chan error)
	ac := appendCmd{args, reply, resultChan}

	ss.nodeBalChan <- &ac

	result := <-resultChan

	LOGV.Println("AppendToList:", ss.nodeID, "Done!, returning", args.Key, result)

	return result
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !ss.isReady {
		LOGE.Println("RemoveFromList:", ss.nodeID, "server is not ready")
		reply.Status = storagerpc.NotReady
		return nil
	}

	LOGV.Println("RemoveFromList:", ss.nodeID, "Recieved remove request",
		args.Key, args.Value)
	if !ss.isKeyOkay(args.Key) {
		LOGE.Println("RemoveFromList:", ss.nodeID, "Wrong Server!", args.Key)
		reply.Status = storagerpc.WrongServer
		return nil
	}

	LOGV.Println("RemoveFromList:", ss.nodeID, "sending request to nodebal",
		args.Key)
	resultChan := make(chan error)
	rc := removeCmd{args, reply, resultChan}

	ss.nodeBalChan <- &rc

	result := <-resultChan

	LOGV.Println("RemoveFromList:", ss.nodeID, "Done!, returning",
		args.Key, result)

	return result
}

//For a given cmd, redirects it to the relevant channel
func (ss *storageServer) nodeBalancer() {
	LOGV.Println("NodeBalancer:", ss.nodeID, "Starting Node Balancer!")
	for {
		select {
		case c := <-ss.nodeBalChan:
			LOGV.Println("NodeBalancer:", ss.nodeID, "Recieved command!")
			key := c.getKey()
			nodeChan, ok := ss.nodeHandlers[key]

			if ok {
				LOGV.Println("NodeBalancer:", ss.nodeID,
					"Sending Command to existing node Handler")
				if nodeChan == nil {
					LOGE.Println("NodeBalancer:", "node chan is nil!")
				}
				nodeChan <- c
			} else {
				LOGV.Println("NodeBalancer:", ss.nodeID, "Initializing node Handler!")
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
