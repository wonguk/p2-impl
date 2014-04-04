package libstore

import (
	"errors"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"

	"github.com/cmu440/tribbler/rpc/librpc"
	"github.com/cmu440/tribbler/rpc/storagerpc"
)

type Nodes []storagerpc.Node

func (n Nodes) Len() int           { return len(n) }
func (n Nodes) Swap(i, j int)      { n[i], n[j] = n[j], n[i] }
func (n Nodes) Less(i, j int) bool { return n[i].NodeID < n[j].NodeID }

var LOGE = log.New(os.Stderr, "ERROR ", log.Lmicroseconds|log.Lshortfile)
var LOGV = log.New(ioutil.Discard, "VERBOSE ", log.Lmicroseconds|log.Lshortfile)

type libstore struct {
	mode         LeaseMode
	masterServer string
	hostport     string

	storageservers Nodes
	storageclients []*rpc.Client

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
	ls := new(libstore)

	ls.mode = mode
	ls.masterServer = masterServerHostPort
	ls.hostport = myHostPort

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
	println("AAAAAAAA   " + masterServerHostPort)
	client, err := rpc.DialHTTP("tcp", masterServerHostPort) //This should attempt to make contact with the master storage server

	if err != nil {
		return nil, errors.New("Could not connect to Storage Server")
	}
	defer client.Close()

	args := new(storagerpc.GetServersArgs) //It's an empty struct
	reply := new(storagerpc.GetServersReply)

	for i := 0; i < 5; i++ {
		err = client.Call("StorageServer.GetServers", args, reply) //Make an rpc to the master server for the other nodes

		if err != nil { //If the call failed then return an error
			return nil, err //errors.New("could not make call with to storage server")
		}

		if reply.Status == storagerpc.OK {
			sort.Sort(Nodes(reply.Servers))
			ls.storageservers = reply.Servers

			rpc.RegisterName("LeaseCallbacks", librpc.Wrap(ls))

			err = ls.initStorageClients()

			return ls, err
		}

		time.Sleep(1 * time.Second)
	}

	return nil, errors.New("failed to connect to storage server 5 times")
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
	client := ls.getStorageClient(key)

	var reply storagerpc.GetReply

	err := client.Call("StorageServer.Get", args, &reply)

	if err != nil {
		return "", err
	}

	// If recieved lease, store to cache
	switch reply.Status {
	case storagerpc.OK:
		if reply.Lease.Granted {
			ls.addToCache(key, reply.Value, nil, reply.Lease.ValidSeconds)
		}

		return reply.Value, nil
	case storagerpc.KeyNotFound:
		return "", errors.New("key not found")
	case storagerpc.ItemNotFound:
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
		return nil
	case storagerpc.KeyNotFound:
		return errors.New("key not found")
	case storagerpc.ItemNotFound:
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
		if reply.Lease.Granted {
			ls.addToCache(key, "", reply.Value, reply.Lease.ValidSeconds)
		}

		return reply.Value, nil
	case storagerpc.KeyNotFound:
		return nil, errors.New("key not found")
	case storagerpc.ItemNotFound:
		return nil, errors.New("item not found")
	case storagerpc.WrongServer:
		return nil, errors.New("wrong server")
	case storagerpc.ItemExists:
		return nil, errors.New("items exist")
	case storagerpc.NotReady:
		return nil, errors.New("not ready")
	default:
		return nil, errors.New("invalid status")
	}
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {
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
		return nil
	case storagerpc.KeyNotFound:
		return errors.New("key not found")
	case storagerpc.ItemNotFound:
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
		return nil
	case storagerpc.KeyNotFound:
		return errors.New("key not found")
	case storagerpc.ItemNotFound:
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
	hash := StoreHash(key)
	index := hash % uint32(len(ls.storageservers))

	return ls.storageclients[index]
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
	cache := make(chan *cache)

	request := cacheRequest{
		key, cache,
	}

	ls.cacheMaster.cacheChan <- &request

	return <-cache
}

func (ls *libstore) queryQuery(key string) bool {
	lease := make(chan bool)

	request := queryRequest{
		key, lease,
	}

	ls.queryMaster.queryChan <- &request

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

func (ls *libstore) initStorageClients() error {
	ls.storageclients = make([]*rpc.Client, len(ls.storageservers))

	for i, node := range ls.storageservers {
		client, err := rpc.DialHTTP("tcp", node.HostPort)

		if err != nil {
			return err
		}

		ls.storageclients[i] = client
	}

	return nil
}
