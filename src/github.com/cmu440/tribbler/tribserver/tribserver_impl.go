package tribserver

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	//"os"
	"io/ioutil"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/tribrpc"
)

var LOGE = log.New(ioutil.Discard, "ERROR ", log.Lmicroseconds|log.Lshortfile)
var LOGV = log.New(ioutil.Discard, "VERBOSE ", log.Lmicroseconds|log.Lshortfile)

type tribServer struct {
	Lib   libstore.Libstore
	ready sync.Mutex
}

//This is here so we can sort arrays of int64
type LongArray struct {
	Array []int64
}

func (a LongArray) Len() int           { return len(a.Array) }
func (a LongArray) Swap(i, j int)      { a.Array[i], a.Array[j] = a.Array[j], a.Array[i] }
func (a LongArray) Less(i, j int) bool { return a.Array[j] < a.Array[i] }

// NewTribServer creates, starts and returns a new TribServer. masterServerHostPort
// is the master storage server's host:port and port is this port number on which
// the TribServer should listen. A non-nil error should be returned if the TribServer
// could not be started.
//
// For hints on how to properly setup RPC, see the rpc/tribrpc package.
func NewTribServer(masterServerHostPort, myHostPort string) (TribServer, error) {
	LOGV.Println("Called newTribServer")
	ts := new(tribServer)
	ts.ready = sync.Mutex{}
	ts.ready.Lock()

	LOGV.Println("Attemping to listen to tcp:", myHostPort)
	listener, err := net.Listen("tcp", myHostPort)
	for err != nil {
		LOGE.Println("Failed to listen to tcp:", myHostPort)
		listener, err = net.Listen("tcp", myHostPort)
	}

	LOGV.Println("Attempting to make an rpc call to RegisterName")
	err = rpc.RegisterName("TribServer", tribrpc.Wrap(ts))
	if err != nil {
		LOGE.Println("Failed at making the rpc call")
		return nil, err
	}

	LOGV.Println("Making a rpc call to HandleHTTP")
	rpc.HandleHTTP()

	LOGV.Println("Spawing a new thread to handle http.Serve")
	go http.Serve(listener, nil)

	ts.Lib, err = libstore.NewLibstore(masterServerHostPort, myHostPort, libstore.Normal)
	LOGV.Println("Exited NewTribServer")
	ts.ready.Unlock()
	return ts, nil
}

func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {
	ts.ready.Lock()
	ts.ready.Unlock()
	LOGV.Println("Called Create User")

	LOGV.Println("Attempting to call Lib.Get to see if there is a user:", args.UserID)
	_, err := ts.Lib.Get(args.UserID) //Want to set that this err should be tribrps.Exists
	if err == nil {
		LOGE.Println("Got an error from Lib.Get")
		reply.Status = tribrpc.Exists
		return err
	}

	LOGV.Println("Putting the userID into the lib server")
	err = ts.Lib.Put(args.UserID, "0") //Make sure type is right
	LOGV.Println("Add the user to the list of clients and setting reply to have ok")
	reply.Status = tribrpc.OK
	LOGV.Println("Exiting Create User")
	return nil
}

func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	ts.ready.Lock()
	ts.ready.Unlock()
	LOGV.Println("Calling Add Subscription", args)

	LOGV.Println("Calling Lib.Get with userID:", args.UserID)
	_, err := ts.Lib.Get(args.UserID)
	if err != nil {
		switch err.(type) {
		case *libstore.KeyNotFound:
			LOGE.Println("Found an error in getting the args.UserID")
			reply.Status = tribrpc.NoSuchUser
			return nil
		default:
			return err
		}
	}

	LOGV.Println("Calling Lib.Get with userID:", args.TargetUserID)
	_, err = ts.Lib.Get(args.TargetUserID)
	if err != nil {
		LOGE.Println("Found an error in getting the args.TargerUserID")
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}

	LOGV.Println("Calling AppendToList with UserID:", args.TargetUserID)
	err = ts.Lib.AppendToList(args.UserID+":Sub", args.TargetUserID)
	if err == nil {
		reply.Status = tribrpc.OK
		return nil
	}
	switch err.(type) {
	case *libstore.ItemExists:
		reply.Status = tribrpc.Exists
		return nil
	default:
		return err
	}
	return nil
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	ts.ready.Lock()
	ts.ready.Unlock()

	LOGV.Println("Calling Remove Subscription")

	LOGV.Println("Calling Lib.Get with userID:", args.UserID)
	_, err := ts.Lib.Get(args.UserID)
	if err != nil {
		switch err.(type) {
		case *libstore.KeyNotFound:
			LOGE.Println("Found an error in getting the args.UserID")
			reply.Status = tribrpc.NoSuchUser
			return nil
		default:
			return err
		}
	}

	LOGV.Println("Calling Lib.Get with TargetUserID:", args.TargetUserID)
	_, err = ts.Lib.Get(args.TargetUserID)
	if err != nil {
		switch err.(type) {
		case *libstore.KeyNotFound:
			LOGE.Println("Found an error in getting the args.TargerUserID")
			reply.Status = tribrpc.NoSuchTargetUser
			return nil
		default:
			return err
		}
	}

	LOGV.Println("Checking if still subscribed")

	LOGV.Println("Calling RemoveFromList with UserID:", args.TargetUserID)
	err = ts.Lib.RemoveFromList(args.UserID+":Sub", args.TargetUserID)
	if err == nil {
		reply.Status = tribrpc.OK
		LOGV.Println("Exiting Remove Subscriptions")
		return nil
	}

	switch err.(type) {
	case *libstore.ItemNotFound:
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	case *libstore.KeyNotFound:
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	default:
		return err
	}
}

func (ts *tribServer) GetSubscriptions(args *tribrpc.GetSubscriptionsArgs, reply *tribrpc.GetSubscriptionsReply) error {
	ts.ready.Lock()
	ts.ready.Unlock()
	LOGV.Println("Calling Get Subscriptons")
	_, err := ts.Lib.Get(args.UserID)
	if err != nil {
		switch err.(type) {
		case *libstore.KeyNotFound:
			reply.Status = tribrpc.NoSuchUser
			return nil
		default:
			return err
		}
	}

	LOGV.Println("Calling Lib.Get to get the subscriptions")
	SubCopy, err := ts.Lib.GetList(args.UserID + ":Sub")
	if err != nil {
		switch err.(type) {
		case *libstore.KeyNotFound:
			//LOGE.Println("Lib.get for subs returned an err")
			//reply.Status = tribrpc.NoSuchUser
			reply.Status = tribrpc.OK
			reply.UserIDs = nil
			return nil
		default:
			return err
		}
	}

	reply.Status = tribrpc.OK
	LOGV.Println(SubCopy)
	reply.UserIDs = SubCopy
	LOGV.Println("Exiting Get Subscriptions")
	return nil

}

func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {
	ts.ready.Lock()
	ts.ready.Unlock()
	LOGV.Println("Calling Get Post Tribble")

	_, err := ts.Lib.Get(args.UserID)
	if err != nil {
		switch err.(type) {
		case *libstore.KeyNotFound:
			reply.Status = tribrpc.NoSuchUser
			return nil
		default:
			return err
		}
	}

	TimeNow := time.Now()
	LOGV.Println("TimeNow:", TimeNow)
	TimeUnix := TimeNow.UnixNano() //This doesnt do what we think it does
	LOGV.Println("TimeUnix:", TimeUnix)
	LOGV.Println("Reverse Test:", time.Unix(TimeUnix/1000, TimeUnix%1000))
	TimeString := strconv.FormatInt(TimeUnix, 10)
	LOGV.Println("TimeString:", TimeString)
	TribString := args.UserID + ":" + TimeString
	LOGV.Println("TribString:", TribString)
	reply.Status = tribrpc.OK
	LOGV.Println("Added add and putting to Tribserver")
	ts.Lib.Put(TribString, args.Contents)
	LOGV.Println("TimeString:", TimeString)
	ts.Lib.AppendToList(args.UserID+":"+"TimeStamps", TimeString)
	//TribTest,_ := ts.Lib.GetList(args.UserID+":"+"TimeStamps")
	//LOGV.Println("TribTest:", TribTest)
	LOGV.Println("Exiting Post Pribble")
	return nil
}

func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	ts.ready.Lock()
	ts.ready.Unlock()
	LOGV.Println("Entering Get Tribbles")

	_, err := ts.Lib.Get(args.UserID)
	if err != nil {
		switch err.(type) {
		case *libstore.KeyNotFound:
			reply.Status = tribrpc.NoSuchUser
			return nil
		default:
			return err
		}
	}

	LOGV.Println("Calling GetList")
	LibList, err := ts.Lib.GetList(args.UserID + ":" + "TimeStamps") //Expect a list of timestamps
	LOGV.Println("LibList:", LibList)
	if err != nil {
		LOGE.Println("Calling GetList failed")
		switch err.(type) {
		case *libstore.KeyNotFound:
			reply.Status = tribrpc.OK //Changed to allow it to pass zero tribs
			return nil
		default:
			return err
		}
	}

	TimeInt := make(subTribs, len(LibList))

	LOGV.Println("Sorting Tribles by time")
	for index, time := range LibList {
		timeInt, err := strconv.ParseInt(time, 10, 64)
		if err != nil {
			LOGE.Println("Ran into an error in time checking loop")
			reply.Status = tribrpc.NoSuchUser
			return err
		}
		TimeInt[index] = subTrib{timeStr: time, time: timeInt}
	}
	LOGV.Println("TimeInt:", TimeInt)
	sort.Sort(subTribs(TimeInt))
	var loopTarget int

	if len(LibList) < 100 {
		loopTarget = len(LibList)
	} else {
		loopTarget = 100
	}

	TribList := make([]tribrpc.Tribble, loopTarget)

	for i, _ := range TribList {
		Trib := new(tribrpc.Tribble)
		Trib.UserID = args.UserID
		Trib.Contents, err = ts.Lib.Get(args.UserID + ":" + TimeInt[i].timeStr)
		Trib.Posted = time.Unix(TimeInt[i].time/1000000000, TimeInt[i].time%1000000000)
		TribList[i] = *Trib
	}

	reply.Status = tribrpc.OK
	reply.Tribbles = TribList
	LOGV.Println("Exiting Get Tribbles")
	return nil
}

type subTrib struct {
	user    string
	timeStr string
	time    int64
}

type subTribs []subTrib

func (s subTribs) Len() int           { return len(s) }
func (s subTribs) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s subTribs) Less(i, j int) bool { return s[j].time < s[i].time }

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	ts.ready.Lock()
	ts.ready.Unlock()
	_, err := ts.Lib.Get(args.UserID)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	SubList, err := ts.Lib.GetList(args.UserID + ":Sub")
	if err != nil {
		switch err.(type) {
		case *libstore.KeyNotFound:
			reply.Status = tribrpc.OK
			return nil
		default:
			return err
		}
	}

	FullList := make(subTribs, 0)

	for _, sub := range SubList {
		SubLibList, _ := ts.Lib.GetList(sub + ":" + "TimeStamps")

		for _, t := range SubLibList {
			time, _ := strconv.ParseInt(t, 10, 64)
			FullList = append(FullList, subTrib{sub, t, time})
		}
	}

	sort.Sort(subTribs(FullList))

	var loopTarget int
	if 100 > len(FullList) {
		loopTarget = len(FullList)
	} else {
		loopTarget = 100
	}

	TribList := make([]tribrpc.Tribble, loopTarget)
	for index := 0; index < loopTarget; index++ {
		Trib := new(tribrpc.Tribble)
		Trib.UserID = FullList[index].user
		timeStr := FullList[index].timeStr
		Trib.Contents, _ = ts.Lib.Get(FullList[index].user + ":" + timeStr)
		Trib.Posted = time.Unix(FullList[index].time/1000000000, FullList[index].time%1000000000)
		TribList[index] = *Trib
	}
	reply.Status = tribrpc.OK
	reply.Tribbles = TribList
	return nil
}
