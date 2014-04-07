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
	"time"

	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/tribrpc"
)

var LOGE = log.New(ioutil.Discard, "ERROR ", log.Lmicroseconds|log.Lshortfile)
var LOGV = log.New(ioutil.Discard, "VERBOSE ", log.Lmicroseconds|log.Lshortfile)

type tribbleUser struct {
	ID string
}

type tribServer struct {
	Clients map[string]tribbleUser //As seen in figure 1 a TribServer must be able to handle multiple TribClients
	Lib     libstore.Libstore
	//Might want to store stuff here depending on Libstore
}

//This is here so we can sort arrays of int64
type LongArray struct {
	Array []int64
}

func (a LongArray) Len() int           { return len(a.Array) }
func (a LongArray) Swap(i, j int)      { a.Array[i], a.Array[j] = a.Array[j], a.Array[i] }
func (a LongArray) Less(i, j int) bool { return a.Array[i] < a.Array[j] }

// NewTribServer creates, starts and returns a new TribServer. masterServerHostPort
// is the master storage server's host:port and port is this port number on which
// the TribServer should listen. A non-nil error should be returned if the TribServer
// could not be started.
//
// For hints on how to properly setup RPC, see the rpc/tribrpc package.
func NewTribServer(masterServerHostPort, myHostPort string) (TribServer, error) {
	LOGV.Println("Called newTribServer")
	ts := new(tribServer)

	LOGV.Println("Attemping to listen to tcp:", myHostPort)
	listener, err := net.Listen("tcp", myHostPort)
	if err != nil {
		LOGE.Println("Failed to listen to tcp:", myHostPort)
		return nil, err
	}

	ts.Clients = make(map[string]tribbleUser)
	LOGV.Println("Calling NewLibstore")
	ts.Lib, err = libstore.NewLibstore(masterServerHostPort, myHostPort, libstore.Normal)

	if err != nil {
		LOGE.Println("Failed to call NewLibstore properly")
		return nil, err
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

	LOGV.Println("Exited NewTribServer")
	return ts, nil
}

func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {
	LOGV.Println("Called Create User")

	LOGV.Println("Attempting to call Lib.Get to see if there is a user:", args.UserID)
	_, err := ts.Lib.Get(args.UserID) //Want to set that this err should be tribrps.Exists
	if err == nil {
		LOGE.Println("Got an error from Lib.Get")
		reply.Status = tribrpc.Exists
		return err
	}

	User := new(tribbleUser)
	User.ID = args.UserID
	LOGV.Println("Putting the userID into the lib server")
	err = ts.Lib.Put(args.UserID, "0") //Make sure type is right
	LOGV.Println("Add the user to the list of clients and setting reply to have ok")
	ts.Clients[User.ID] = *User
	reply.Status = tribrpc.OK
	LOGV.Println("Exiting Create User")
	return nil
}

func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	LOGV.Println("Calling Add Subscription", args)

	LOGV.Println("Calling Lib.Get with userID:", args.UserID)
	_, err := ts.Lib.Get(args.UserID)
	if err != nil {
		LOGE.Println("Found an error in getting the args.UserID")
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	LOGV.Println("Checking if the Target user hasnt been added")
	DupCheck, _ := ts.Lib.GetList(args.UserID + ":Sub")

	for index := 0; index < len(DupCheck); index++ {
		if DupCheck[index] == args.TargetUserID {
			reply.Status = tribrpc.Exists
			return nil
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
	ts.Lib.AppendToList(args.UserID+":Sub", args.TargetUserID)
	//PrintList,_ := ts.Lib.GetList(args.UserID+":Sub")
	//LOGV.Println("Checking list:", PrintList)
	reply.Status = tribrpc.OK
	LOGV.Println("Exiting Add Subscription")
	return nil
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {

	LOGV.Println("Calling Remove Subscription")

	LOGV.Println("Calling Lib.Get with userID:", args.UserID)
	_, err := ts.Lib.Get(args.UserID)
	if err != nil {
		LOGE.Println("Found an error in getting the args.UserID")
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	LOGV.Println("Calling Lib.Get with TargetUserID:", args.TargetUserID)
	_, err = ts.Lib.Get(args.TargetUserID)
	if err != nil {
		LOGE.Println("Found an error in getting the args.TargerUserID")
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}

	LOGV.Println("Checking if still subscribed")
	DupList, _ := ts.Lib.GetList(args.UserID + ":Sub")
	LOGV.Println(DupList)
	if len(DupList) == 0 {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}
	for index := 0; index < len(DupList); index++ {
		if DupList[index] == args.TargetUserID {
			break
		}
		if index == len(DupList)-1 {
			reply.Status = tribrpc.NoSuchTargetUser
			return nil
		}
	}

	LOGV.Println("Calling RemoveFromList with UserID:", args.TargetUserID)
	ts.Lib.RemoveFromList(args.UserID+":Sub", args.TargetUserID)
	//CheckList,_ := ts.Lib.GetList(args.UserID+":Sub")
	//LOGV.Println(CheckList)
	reply.Status = tribrpc.OK
	LOGV.Println("Exiting Remove Subscriptions")
	return nil
}

func (ts *tribServer) GetSubscriptions(args *tribrpc.GetSubscriptionsArgs, reply *tribrpc.GetSubscriptionsReply) error {
	LOGV.Println("Calling Get Subscriptons")
	UserCheck := ts.Clients[args.UserID]
	LOGV.Println("Checking for User info:", UserCheck)
	if UserCheck.ID == "" {
		LOGE.Println("Invalid UserID,return error")
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	LOGV.Println("Calling Lib.Get to get the subscriptions")
	SubCopy, err := ts.Lib.GetList(args.UserID + ":Sub")
	if err != nil {
		LOGE.Println("Lib.get for subs returned an err")
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	reply.Status = tribrpc.OK
	LOGV.Println(SubCopy)
	reply.UserIDs = SubCopy
	LOGV.Println("Exiting Get Subscriptions")
	return nil

}

func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {
	LOGV.Println("Calling Get Post Tribble")
	UserCheck := ts.Clients[args.UserID]

	LOGV.Println("Checking for User info:", UserCheck)
	if UserCheck.ID == "" {
		LOGV.Println("Invalid UserID,return error")
		reply.Status = tribrpc.NoSuchUser
		return nil
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
	LOGV.Println("Entering Get Tribbles")
	UserCheck := ts.Clients[args.UserID]
	LOGV.Println(UserCheck)
	if UserCheck.ID == "" {
		LOGV.Println("Failed looking for a user in ts.Clients")
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	LOGV.Println("Calling GetList")
	LibList, err := ts.Lib.GetList(args.UserID + ":" + "TimeStamps") //Expect a list of timestamps
	LOGV.Println("LibList:", LibList)
	if err != nil {
		LOGE.Println("Calling GetList failed")
		reply.Status = tribrpc.OK //Changed to allow it to pass zero tribs
		return nil
	}

	TimeInt := make([]int64, len(LibList))
	LOGV.Println("Sorting Tribles by time")
	for index := 0; index < len(LibList); index++ {
		TimeInt[index], err = strconv.ParseInt(LibList[index], 10, 64)
		if err != nil {
			LOGE.Println("Ran into an error in time checking loop")
			reply.Status = tribrpc.NoSuchUser
			return nil
		}
	}
	LOGV.Println("TimeInt:", TimeInt)
	LongArrayTest := new(LongArray)
	LongArrayTest.Array = TimeInt
	sort.Sort(LongArrayTest)
	TimeInt = LongArrayTest.Array
	LOGV.Println("TimeInt Sorted:", TimeInt)
	var loopTarget int
	if 100 >= len(LibList) {
		loopTarget = len(LibList)
		LOGV.Println("LoopTarget:", loopTarget)
		TribList := make([]tribrpc.Tribble, loopTarget)
		for index := 0; index < loopTarget; index++ {
			Trib := new(tribrpc.Tribble)
			Trib.UserID = args.UserID
			Trib.Contents, _ = ts.Lib.Get(args.UserID + ":" + strconv.FormatInt(TimeInt[index], 10))
			Trib.Posted = time.Unix(TimeInt[index]/1000, TimeInt[index]%1000)
			// fmt.Println("Got Trib:",Trib)
			TribList[loopTarget-index-1] = *Trib
		}
		reply.Status = tribrpc.OK
		reply.Tribbles = TribList
	} else {
		loopTarget = 100
		LOGV.Println("Len over thus LoopTager = 100")
		TribList := make([]tribrpc.Tribble, loopTarget)
		for index := 0; index < 100; index++ {
			Trib := new(tribrpc.Tribble)
			Trib.UserID = args.UserID
			ModPosition := len(LibList) - index - 1
			ModPosition = index + (len(LibList) - 100)
			Trib.Contents, _ = ts.Lib.Get(args.UserID + ":" + strconv.FormatInt(TimeInt[ModPosition], 10))
			Trib.Posted = time.Unix(TimeInt[index]/1000, TimeInt[index]%1000)
			TribList[99-index] = *Trib
		}
		TribListTest := make([]tribrpc.Tribble, loopTarget)
		for index := 0; index < 100; index++ {
			//TribListTest[index] = TribList[99-index]
			TribListTest[index] = TribList[index]
		}

		reply.Status = tribrpc.OK
		reply.Tribbles = TribListTest
	}
	LOGV.Println("Exiting Get Tribbles")
	return nil
}

type subTrib struct {
	user string
	time int64
}

type subTribs []subTrib

func (s subTribs) Len() int           { return len(s) }
func (s subTribs) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s subTribs) Less(i, j int) bool { return s[j].time < s[i].time }

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	UserCheck := ts.Clients[args.UserID]
	if UserCheck.ID == "" {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	SubList, err := ts.Lib.GetList(args.UserID + ":Sub")
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	FullList := make(subTribs, 0)

	for _, sub := range SubList {
		SubLibList, errLoop := ts.Lib.GetList(sub + ":" + "TimeStamps")
		if errLoop != nil {
			reply.Status = tribrpc.NoSuchTargetUser
			return nil
		}

		for _, t := range SubLibList {
			time, _ := strconv.ParseInt(t, 10, 64)
			FullList = append(FullList, subTrib{sub, time})
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
		timeStr := strconv.FormatInt(FullList[index].time, 10)
		Trib.Contents, _ = ts.Lib.Get(FullList[index].user + ":" + timeStr)
		Trib.Posted = time.Unix(FullList[index].time, 0)
		TribList[index] = *Trib
	}
	reply.Status = tribrpc.OK
	reply.Tribbles = TribList
	return nil
}
