package libstore

import (
	"errors"
	"io/ioutil"
	"log"
	"net/rpc"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cmu440/tribbler/rpc/librpc"
	"github.com/cmu440/tribbler/rpc/storagerpc"
)

type Nodes []storagerpc.Node

func (n Nodes) Len() int           { return len(n) }
func (n Nodes) Swap(i, j int)      { n[i], n[j] = n[j], n[i] }
func (n Nodes) Less(i, j int) bool { return n[i].NodeID < n[j].NodeID }

var LOGE = log.New(ioutil.Discard, "ERROR ", log.Lmicroseconds|log.Lshortfile)
var LOGV = log.New(ioutil.Discard, "VERBOSE ", log.Lmicroseconds|log.Lshortfile)

type libstore struct {
	mode         LeaseMode
	masterServer string
	hostport     string

	storageservers Nodes
	storageclients []*rpc.Client
	storageLock    sync.Mutex

	queryMaster *queryMaster
	cacheMaster *cacheMaster
}

// NewLibstore creates a new instance of a TribServer's libstore. masterServerHostPort
// is the master storage server's host:port. myHostPort is this Libstore's host:port
// (i.e. the callback address that the storage servers should use to send back
// notifications when leases are revoked).
//
// The mode argument is a debugging flag that determines how the Libstore should
// request/handle leases. If mode is Never, then the Libstore should never request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to false). If mode is Always, then the Libstore should always request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to true). If mode is Normal, then the Libstore should make its own
// decisions on whether or not a lease should be requested from the storage server,
// based on the requirements specified in the project PDF handout.  Note that the
// value of the mode flag may also determine whether or not the Libstore should
// register to receive RPCs from the storage servers.
//
// To register the Libstore to receive RPCs from the storage servers, the following
// line of code should suffice:
//
//     rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore))
//
// Note that unlike in the NewTribServer and NewStorageServer functions, there is no
// need to create a brand new HTTP handler to serve the requests (the Libstore may
// simply reuse the TribServer's HTTP handler since the two run in the same process).
func NewLibstore(masterServerHostPort, myHostPort string, mode LeaseMode) (Libstore, error) {
	LOGV.Println("[LIB]", "NewLibstore:", "Initializing....")
	ls := new(libstore)

	ls.mode = mode
	ls.masterServer = masterServerHostPort
	ls.hostport = myHostPort
	ls.storageLock = sync.Mutex{}

	qm := new(queryMaster)

	qm.queryMap = make(map[string]*queryCell)
	qm.deleteChan = make(chan string)
	qm.queryChan = make(chan *queryRequest)

	go qm.startQueryMaster()

	cm := new(cacheMaster)

	cm.cacheMap = make(map[string]*cacheCell)
	cm.cacheChan = make(chan *cacheRequest)
	cm.newCacheChan = make(chan *cacheCell)
	cm.revokeCacheChan = make(chan *revokeRequest)
	cm.deleteCacheChan = make(chan string)

	go cm.startCacheMaster()

	ls.queryMaster = qm
	ls.cacheMaster = cm

	//Get storage server addresses, and sort them by NodeID
	client, err := rpc.DialHTTP("tcp", masterServerHostPort) //This should attempt to make contact with the master storage server

	if err != nil {
		LOGE.Println("[LIB]", "NewLibstore:", "Failed to connect to storage server", err)
		return nil, errors.New("Could not connect to Storage Server")
	}
	defer client.Close()

	args := new(storagerpc.GetServersArgs) //It's an empty struct
	reply := new(storagerpc.GetServersReply)

	for i := 0; i < 5; i++ {
		err = client.Call("StorageServer.GetServers", args, reply) //Make an rpc to the master server for the other nodes

		if err != nil { //If the call failed then return an error
			LOGE.Println("[LIB]", "NewLibstore:", "Error calling GetServers", err)
			return nil, err
		}

		if reply.Status == storagerpc.OK {
			LOGV.Println("[LIB]", "NewLibstore:", "Recieved list of servers")
			sort.Sort(Nodes(reply.Servers))
			ls.storageservers = reply.Servers
			ls.storageclients = make([]*rpc.Client, len(ls.storageservers))

			rpc.RegisterName("LeaseCallbacks", librpc.Wrap(ls))

			//err = ls.initStorageClients()

			LOGV.Println("[LIB]", "NewLibstore:", "Done!")
			return ls, err
		}

		time.Sleep(1 * time.Second)
	}

	LOGE.Println("[LIB]", "NewLibstore:", "failed to connect to server 5 times")
	return nil, errors.New("failed to connect to storage server 5 times")
}

func (ls *libstore) Get(key string) (string, error) {
	LOGV.Println("[LIB]", "Get:", key)
	cache := ls.queryCache(key)

	if cache != nil {
		return cache.data, nil
	}

	lease := ls.requestLease(key)

	args := storagerpc.GetArgs{
		Key:       key,
		WantLease: lease,
		HostPort:  ls.hostport,
	}

	// Make rpc call to storage server
	client := ls.getStorageClient(key)

	var reply storagerpc.GetReply

	err := client.Call("StorageServer.Get", args, &reply)

	if err != nil {
		return "", err
	}

	// If recieved lease, store to cache
	switch reply.Status {
	case storagerpc.OK:
		LOGV.Println("[LIB]", "Get:", "(OK)", key, reply.Value)
		if reply.Lease.Granted {
			LOGV.Println("[LIB]", "Get:", "(OK)", "got lease", key, reply.Value)
			ls.addToCache(key, reply.Value, nil, reply.Lease.ValidSeconds)
		}

		return reply.Value, nil
	case storagerpc.KeyNotFound:
		LOGE.Println("[LIB]", "Get:", "(KeyNotFound)", key)
		return "", new(KeyNotFound)
	case storagerpc.ItemNotFound:
		LOGE.Println("[LIB]", "Get:", "(ItemNotFound)", key)
		return "", new(ItemNotFound)
	case storagerpc.WrongServer:
		LOGE.Println("[LIB]", "Get:", "(WrongServer)", key)
		return "", new(WrongServer)
	case storagerpc.ItemExists:
		LOGE.Println("[LIB]", "Get:", "(ItemExists)", key)
		return "", new(ItemExists)
	case storagerpc.NotReady:
		LOGE.Println("[LIB]", "Get:", "(NotReady)", key)
		return "", new(NotReady)
	default:
		LOGE.Println("[LIB]", "Get:", "(invalid status)", key)
		return "", new(InvalidStatus)
	}
}

func (ls *libstore) Put(key, value string) error {
	LOGV.Println("[LIB]", "Put:", key, value)
	client := ls.getStorageClient(key)

	args := storagerpc.PutArgs{
		key, value,
	}

	var reply storagerpc.PutReply

	err := client.Call("StorageServer.Put", args, &reply)
	if err != nil {
		return err
	}

	switch reply.Status {
	case storagerpc.OK:
		LOGV.Println("[LIB]", "Put:", "(OK)", key, value)
		return nil
	case storagerpc.KeyNotFound:
		LOGE.Println("[LIB]", "Put:", "(KeyNotFound)", key, value)
		return new(KeyNotFound)
	case storagerpc.ItemNotFound:
		LOGE.Println("[LIB]", "Put:", "(ItemNotFound)", key, value)
		return new(ItemNotFound)
	case storagerpc.WrongServer:
		LOGE.Println("[LIB]", "Put:", "(WrongServer)", key, value)
		return new(WrongServer)
	case storagerpc.ItemExists:
		LOGE.Println("[LIB]", "Put:", "(ItemExists)", key, value)
		return new(ItemExists)
	case storagerpc.NotReady:
		LOGE.Println("[LIB]", "Put:", "(NotReady)", key, value)
		return new(NotReady)
	default:
		LOGE.Println("[LIB]", "Put:", "invalid status", key, value)
		return new(InvalidStatus)
	}
}

func (ls *libstore) GetList(key string) ([]string, error) {
	LOGV.Println("[LIB]", "GetList", key)
	cache := ls.queryCache(key)

	if cache != nil {
		return cache.listData, nil
	}

	lease := ls.requestLease(key)

	args := storagerpc.GetArgs{
		Key:       key,
		WantLease: lease,
		HostPort:  ls.hostport,
	}

	// Make rpc call to storage server
	client := ls.getStorageClient(key)

	var reply storagerpc.GetListReply

	err := client.Call("StorageServer.GetList", args, &reply)

	if err != nil {
		return nil, err
	}

	// If recieved lease, store to cache
	switch reply.Status {
	case storagerpc.OK:
		LOGV.Println("[LIB]", "GetList:", "(OK)", key, reply.Value)
		if reply.Lease.Granted {
			LOGV.Println("[LIB]", "GetList:", "(OK)", "got lease", key, reply.Value)
			ls.addToCache(key, "", reply.Value, reply.Lease.ValidSeconds)
		}

		return reply.Value, nil
	case storagerpc.KeyNotFound:
		LOGE.Println("[LIB]", "GetList:", "(KeyNotFound)", key)
		return nil, new(KeyNotFound)
	case storagerpc.ItemNotFound:
		LOGE.Println("[LIB]", "GetList:", "(ItemNotFound)", key)
		return nil, new(ItemNotFound)
	case storagerpc.WrongServer:
		LOGE.Println("[LIB]", "GetList:", "(WrongServer)", key)
		return nil, new(WrongServer)
	case storagerpc.ItemExists:
		LOGE.Println("[LIB]", "GetList:", "(ItemExists)", key)
		return nil, new(ItemExists)
	case storagerpc.NotReady:
		LOGE.Println("[LIB]", "GetList:", "(NotReady)", key)
		return nil, new(NotReady)
	default:
		LOGE.Println("[LIB]", "GetList:", "invalid status", key)
		return nil, new(InvalidStatus)
	}
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {
	LOGV.Println("[LIB]", "RemoveFromList", key, removeItem)
	client := ls.getStorageClient(key)

	args := storagerpc.PutArgs{
		key, removeItem,
	}

	var reply storagerpc.PutReply

	err := client.Call("StorageServer.RemoveFromList", args, &reply)
	if err != nil {
		return err
	}

	switch reply.Status {
	case storagerpc.OK:
		LOGV.Println("[LIB]", "RemoveFromList:", "(OK)", key, removeItem)
		return nil
	case storagerpc.KeyNotFound:
		LOGE.Println("[LIB]", "RemoveFromList:", "(KeyNotFound)", key, removeItem)
		return new(KeyNotFound)
	case storagerpc.ItemNotFound:
		LOGE.Println("[LIB]", "RemoveFromList:", "(ItemNotFound)", key, removeItem)
		return new(ItemNotFound)
	case storagerpc.WrongServer:
		LOGE.Println("[LIB]", "RemoveFromList:", "(WrongServer)", key, removeItem)
		return new(WrongServer)
	case storagerpc.ItemExists:
		LOGE.Println("[LIB]", "RemoveFromList:", "(ItemExists)", key, removeItem)
		return new(ItemExists)
	case storagerpc.NotReady:
		LOGE.Println("[LIB]", "RemoveFromList:", "(NotReady)", key, removeItem)
		return new(NotReady)
	default:
		LOGE.Println("[LIB]", "RemoveFromList:", "invalid status", key, removeItem)
		return new(InvalidStatus)
	}
}

func (ls *libstore) AppendToList(key, newItem string) error {
	LOGV.Println("[LIB]", "AppendToList", key, newItem)
	client := ls.getStorageClient(key)

	args := storagerpc.PutArgs{
		key, newItem,
	}

	var reply storagerpc.PutReply

	err := client.Call("StorageServer.AppendToList", args, &reply)
	if err != nil {
		return err
	}

	switch reply.Status {
	case storagerpc.OK:
		LOGV.Println("[LIB]", "AppendToList:", "(OK)", key, newItem)
		return nil
	case storagerpc.KeyNotFound:
		LOGE.Println("[LIB]", "AppendToList:", "(KeyNotFound)", key, newItem)
		return new(KeyNotFound)
	case storagerpc.ItemNotFound:
		LOGE.Println("[LIB]", "AppendToList:", "(ItemNotFound)", key, newItem)
		return new(ItemNotFound)
	case storagerpc.WrongServer:
		LOGE.Println("[LIB]", "AppendToList:", "(WrongServer)", key, newItem)
		return new(WrongServer)
	case storagerpc.ItemExists:
		LOGE.Println("[LIB]", "AppendToList:", "(ItemExists)", key, newItem)
		return new(ItemExists)
	case storagerpc.NotReady:
		LOGE.Println("[LIB]", "AppendToList:", "(NotReady)", key, newItem)
		return new(NotReady)
	default:
		LOGE.Println("[LIB]", "AppendToList:", "invalid status", key, newItem)
		return new(InvalidStatus)
	}
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	LOGV.Println("[LIB]", "RevokeLease:", "recieved request to revoke", args.Key)
	ok := make(chan bool)
	req := revokeRequest{
		args.Key, ok,
	}

	LOGV.Println("[LIB]", "RevokeLease:", "sending request to cacheMaster", args.Key)
	ls.cacheMaster.revokeCacheChan <- &req

	if <-ok {
		LOGV.Println("[LIB]", "RevokeLease:", args.Key, "has been revoked")
		reply.Status = storagerpc.OK
	} else {
		LOGV.Println("[LIB]", "RevokeLease:", args.Key, "was not found")
		reply.Status = storagerpc.ItemNotFound
	}

	return nil
}

// Given the key, figures out the address of the relevant storage server
func (ls *libstore) getStorageClient(key string) *rpc.Client {
	hash := StoreHash(strings.Split(key, ":")[0])

	for i, s := range ls.storageservers {
		if s.NodeID >= hash {
			LOGV.Println("[LIB]", "getStorageClient:", "Routing to", s.NodeID)
			return ls.getClient(i)
		}
	}

	LOGV.Println("[LIB]", "getStorageClient:", "Routing to",
		ls.storageservers[0].NodeID)

	return ls.getClient(0)
}

func (ls *libstore) getClient(i int) *rpc.Client {
	ls.storageLock.Lock()
	defer ls.storageLock.Unlock()

	if ls.storageclients[i] == nil {
		client, err := rpc.DialHTTP("tcp", ls.storageservers[i].HostPort)

		for err != nil {
			LOGE.Println("[LIB]", "NewLibstore:", "Error initializing client",
				ls.storageservers[i].NodeID, err)

			time.Sleep(time.Second)

			client, err = rpc.DialHTTP("tcp", ls.storageservers[i].HostPort)
		}

		ls.storageclients[i] = client
	}

	return ls.storageclients[i]
}

func (ls *libstore) requestLease(key string) bool {
	switch ls.mode {
	case Never:
		return false
	case Always:
		return true
	case Normal:
		return ls.queryQuery(key)
	default:
		return false
	}
}

func (ls *libstore) queryCache(key string) *cache {
	LOGV.Println("[LIB]", "queryCache:", "Querying cache for", key)
	cache := make(chan *cache)

	request := cacheRequest{
		key, cache,
	}

	ls.cacheMaster.cacheChan <- &request

	return <-cache
}

func (ls *libstore) queryQuery(key string) bool {
	LOGV.Println("[LIB]", "queryQuery:", "Querying lease for", key)
	lease := make(chan bool)

	request := queryRequest{
		key, lease,
	}

	ls.queryMaster.queryChan <- &request

	return <-lease
}

func (ls *libstore) addToCache(key, value string, listValue []string, duration int) {
	LOGV.Println("[LIB]", "addToCache:", "Adding", key, "to cache")
	cc := new(cacheCell)

	cc.key = key
	cc.value = value
	cc.listValue = listValue
	cc.duration = duration

	cc.reqChan = make(chan *cacheRequest)
	cc.delChan = make(chan struct{})

	ls.cacheMaster.newCacheChan <- cc
}

/*
func (ls *libstore) initStorageClients() error {
	LOGV.Println("[LIB]", "initStorageClients:", "initializing clients...")
	ls.storageclients = make([]*rpc.Client, len(ls.storageservers))

	for i, node := range ls.storageservers {
		client, err := rpc.DialHTTP("tcp", node.HostPort)

		for err != nil {
			LOGE.Println("[LIB]", "NewLibstore:", "Error initializing client",
				node.NodeID, err)

			time.Sleep(time.Second)

			client, err = rpc.DialHTTP("tcp", node.HostPort)
		}

		ls.storageclients[i] = client
	}

	args := new(storagerpc.GetServersArgs)
	reply := new(storagerpc.GetServersReply)

	for _, client := range ls.storageclients {
		client.Call("StorageServer.GetServers", args, reply)

		for reply.Status != storagerpc.OK {
			time.Sleep(time.Second)
			client.Call("StorageServer.GetServers", args, reply)
		}
	}

	return nil
}*/
