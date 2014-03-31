package libstore

import (
	"errors"
	"net/rpc"

	"github.com/cmu440/tribbler/rpc/librpc"
	"github.com/cmu440/tribbler/rpc/storagerpc"
)

type libstore struct {
	mode         LeaseMode
	masterServer string
	hostport     string

	storageservers []storagerpc.Node

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
	ls = new(libstore)

	ls.mode = mode
	ls.masterServer = masterServerHostPort
	ls.hostport = myHostPort

	rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore))

	qm = new(queryMaster)

	qm.queryMap = make(map[string]*queryCell)
	qm.delChan = make(chan string)
	qm.queryChan = make(chan *queryRequest)

	go qm.startQueryMaster()

	cm = new(cacheMaster)

	cm.cacheMap = make(map[string]chan *cacheRequest)
	cm.cacheChan = make(chan *cacheRequest)
	cm.newCacheChan = make(chan *cacheCell)
	cm.delCacheChan = make(chan string)

	go cm.startCacheMaster()

	ls.queryMaster = qm
	ls.cacheMaster = cm

	//TODO Get storage server addresses, and sort them by NodeID
	
	//As seen on piazza in "p2:Golan RPC"

	client,err := rpc.DialHTTP("tcp",masterServerHostPort) //This should attempt to make contact with the master storage server

	if err != nil {
		return nil,errors.New("Could not connect to Storage Server")
	}

	StorageArgs := new(storagerpc.GetServersArgs) //It's an empty struct
	StorageReply := new(storagerpc.GetServersReply)

	err = client.Call("GetServers",&StorageArgs,&StorageReply) //Make an rpc to the master server for the other nodes

	if err != nil { //If the call failed then return an error
		return nil,errors.New("Could not make call with to Storage Server")
	}

	//Not we organize the array of nodes to be sorted
		//Note: We might want to change this to be done as added as this takes alot of ops

	//Get a list of the client IDs from all the nodes
	ServerList := StorageReply.Servers
	IndexList := make([]uint32,len(ServerList))
	for index := 0; index < len(ServerCopy); index ++ {
		NodeCopy := ServerList[index]
		IndexList[index] = NodeCopy.NodeID
	}
	IndexList.sort() //Sort the UserID's

	ServerList = make([]storagerpc.Node,len(StorageReply.Servers)) //Set ServerList to be a new empty array of same size
	for ListIndex := 0; ListIndex < len(IndexList); ListIndex++ { //The index of the sorted list
		for ServerIndex := 0 ; ServerIndex < len(IndexList); ServerList++ {
			NodeCopy := ServerList[index]
			if NodeCopy.NodeID == ListIndex[ListIndex] { //This is assuming the NodeID are generic
				ServerList[ListIndex] = NodeCopy
			}
		}
	}

	ls.storageservers = ServerList
	
	return ls
}

func (ls *libstore) Get(key string) (string, error) {
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
	client, err := rpc.DialHTTP("tcp", ls.hostport)
	if err != nil {
		return "", err
	}

	var reply storagerpc.GetReply

	err = client.Call("RemoteStorageServer.Get", args, &reply)

	if err != nil {
		return "", err
	}

	// If recieved lease, store to cache
	switch reply.Status {
	case storagerpc.Ok:
		if reply.Lease.Granted {
			ls.addToCache(key, reply.Value, nil, reply.Lease.ValidSeconds)
		}

		return reply.Value, nil
	case storagerpc.KeyNotFound:
		return "", errors.New("key not found")
	case storagerpc.ItemNotFoun:
		return "", errors.New("item not found")
	case storagerpc.WrongServer:
		return "", errors.New("wrong server")
	case storagerpc.ItemExists:
		return "", errors.New("items exist")
	case storagerpc.NotReady:
		return "", errors.New("not ready")
	default:
		return "", errors.New("invalid status")
	}
}

func (ls *libstore) Put(key, value string) error {
	client, err := rpc.DialHTTP("tcp", ls.hostport)
	if err != nil {
		return err
	}

	args := storagerpc.PutArgs{
		key, value,
	}

	var reply storagerpc.PutReply

	err = client.Call("RemoteStorageServer.Put", args, &reply)
	if err != nil {
		return err
	}

	switch reply.Status {
	case storagerpc.Ok:
		return nil
	case storagerpc.KeyNotFound:
		return errors.New("key not found")
	case storagerpc.ItemNotFoun:
		return errors.New("item not found")
	case storagerpc.WrongServer:
		return errors.New("wrong server")
	case storagerpc.ItemExists:
		return errors.New("items exist")
	case storagerpc.NotReady:
		return errors.New("not ready")
	default:
		return errors.New("invalid status")
	}
}

func (ls *libstore) GetList(key string) ([]string, error) {
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
	client, err := rpc.DialHTTP("tcp", ls.hostport)
	if err != nil {
		return "", err
	}

	var reply storagerpc.GetListReply

	err = client.Call("RemoteStorageServer.GetList", args, &reply)

	if err != nil {
		return "", err
	}

	// If recieved lease, store to cache
	switch reply.Status {
	case storagerpc.Ok:
		if reply.Lease.Granted {
			ls.addToCache(key, "", reply.Value, reply.Lease.ValidSeconds)
		}

		return reply.Value, nil
	case storagerpc.KeyNotFound:
		return "", errors.New("key not found")
	case storagerpc.ItemNotFoun:
		return "", errors.New("item not found")
	case storagerpc.WrongServer:
		return "", errors.New("wrong server")
	case storagerpc.ItemExists:
		return "", errors.New("items exist")
	case storagerpc.NotReady:
		return "", errors.New("not ready")
	default:
		return "", errors.New("invalid status")
	}
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {
	client, err := rpc.DialHTTP("tcp", ls.hostport)
	if err != nil {
		return err
	}

	args := storagerpc.PutArgs{
		key, removeItem,
	}

	var reply storagerpc.PutReply

	err = client.Call("RemoteStorageServer.RemoveFromList", args, &reply)
	if err != nil {
		return err
	}

	switch reply.Status {
	case storagerpc.Ok:
		return nil
	case storagerpc.KeyNotFound:
		return errors.New("key not found")
	case storagerpc.ItemNotFoun:
		return errors.New("item not found")
	case storagerpc.WrongServer:
		return errors.New("wrong server")
	case storagerpc.ItemExists:
		return errors.New("items exist")
	case storagerpc.NotReady:
		return errors.New("not ready")
	default:
		return errors.New("invalid status")
	}
}

func (ls *libstore) AppendToList(key, newItem string) error {
	client, err := rpc.DialHTTP("tcp", ls.hostport)
	if err != nil {
		return err
	}

	args := storagerpc.PutArgs{
		key, newItem,
	}

	var reply storagerpc.PutReply

	err = client.Call("RemoteStorageServer.AppendToList", args, &reply)
	if err != nil {
		return err
	}

	switch reply.Status {
	case storagerpc.Ok:
		return nil
	case storagerpc.KeyNotFound:
		return errors.New("key not found")
	case storagerpc.ItemNotFoun:
		return errors.New("item not found")
	case storagerpc.WrongServer:
		return errors.New("wrong server")
	case storagerpc.ItemExists:
		return errors.New("items exist")
	case storagerpc.NotReady:
		return errors.New("not ready")
	default:
		return errors.New("invalid status")
	}
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	ok := make(chan bool)
	req := revokeRequest{
		key, ok,
	}

	ls.cacheMaster.revokeCacheChan <- &req

	if <-ok {
		reply.Status = storagerpc.Ok
	} else {
		reply.Status = storagerpc.ItemNotFound
	}

	return nil
}

// Given the key, figures out the address of the relevant storage server
func (ls *libstore) getStorateServer(key string) string {
	hash := StireHash(key)

	return ls.storageservers[hash%len(ls.storageservers)]
}

func (ls *libstore) requestLease(key string) bool {
	switch ls.mode {
	case Never:
		return false
	case Always:
		return true
	case Normal:
		return ls.queryQuery(key)
	}
}

func (ls *libstore) queryCache(key string) *cache {
	cache := make(chan *Cache)

	request := cacheRequest{
		key, cache,
	}

	ls.cacheChan <- request

	return <-cache
}

func (ls *libstore) queryQuery(key string) bool {
	lease := make(chan bool)

	request := queryRequest{
		key, lease,
	}

	ls.queryChan <- request

	return <-lease
}

func (ls *libstore) addToCache(key, value string, listValue []string, duration int) {
	cc := new(cacheCell)

	cc.key = key
	cc.value = value
	cc.listValue = listValue
	cc.duration = duration

	cc.reqChan = make(chan *cacheRequest)
	cc.delChan = make(chan struct{})

	ls.cacheMaster.newCacheChan <- cc
}
