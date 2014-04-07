package tribserver

import (
	//"errors"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"sort"
	"strconv"
	//"strings"
	"time"
	//"math"

	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/tribrpc"
	//"github.com/cmu440/tribbler/tribclient"
)

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
	//fmt.Println("Called newTribServer")
	ts := new(tribServer)

	//fmt.Println("Attemping to listen to tcp:", myHostPort)
	listener, err := net.Listen("tcp", myHostPort)
	if err != nil {
		fmt.Println("Failed to listen to tcp:", myHostPort)
		return nil, err
	}

	ts.Clients = make(map[string]tribbleUser)
	//fmt.Println("Calling NewLibstore")
	ts.Lib, err = libstore.NewLibstore(masterServerHostPort, myHostPort, libstore.Normal)

	if err != nil {
		fmt.Println("Failed to call NewLibstore properly")
		return nil, err
	}

	//fmt.Println("Attempting to make an rpc call to RegisterName")
	err = rpc.RegisterName("TribServer", tribrpc.Wrap(ts))
	if err != nil {
		fmt.Println("Failed at making the rpc call")
		return nil, err
	}

	//fmt.Println("Making a rpc call to HandleHTTP")
	rpc.HandleHTTP()

	//fmt.Println("Spawing a new thread to handle http.Serve")
	go http.Serve(listener, nil)

	//fmt.Println("Exited NewTribServer")
	return ts, nil
}

func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {
	//fmt.Println("Called Create User")

	//fmt.Println("Attempting to call Lib.Get to see if there is a user:", args.UserID)
	_, err := ts.Lib.Get(args.UserID) //Want to set that this err should be tribrps.Exists
	if err == nil {
		fmt.Println("Got an error from Lib.Get")
		reply.Status = tribrpc.Exists
		return err
	}

	User := new(tribbleUser)
	User.ID = args.UserID
	//fmt.Println("Putting the userID into the lib server")
	err = ts.Lib.Put(args.UserID, "0") //Make sure type is right
	//fmt.Println("Add the user to the list of clients and setting reply to have ok")
	ts.Clients[User.ID] = *User
	reply.Status = tribrpc.OK
	//fmt.Println("Exiting Create User")
	return nil
}

func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	//fmt.Println("Calling Add Subscription", args)

	//fmt.Println("Calling Lib.Get with userID:", args.UserID)
	_, err := ts.Lib.Get(args.UserID)
	if err != nil {
		fmt.Println("Found an error in getting the args.UserID")
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	//fmt.Println("Checking if the Target user hasnt been added")
	DupCheck, _ := ts.Lib.GetList(args.UserID + ":Sub")

	for index := 0; index < len(DupCheck); index++ {
		if DupCheck[index] == args.TargetUserID {
			reply.Status = tribrpc.Exists
			return nil
		}
	}

	//fmt.Println("Calling Lib.Get with userID:", args.TargetUserID)
	_, err = ts.Lib.Get(args.TargetUserID)
	if err != nil {
		//fmt.Println("Found an error in getting the args.TargerUserID")
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}

	//fmt.Println("Calling AppendToList with UserID:", args.TargetUserID)
	ts.Lib.AppendToList(args.UserID+":Sub", args.TargetUserID)
	//PrintList,_ := ts.Lib.GetList(args.UserID+":Sub")
	//fmt.Println("Checking list:",PrintList)
	reply.Status = tribrpc.OK
	//fmt.Println("Exiting Add Subscription")
	return nil
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {

	//fmt.Println("Calling Remove Subscription")

	//fmt.Println("Calling Lib.Get with userID:", args.UserID)
	_, err := ts.Lib.Get(args.UserID)
	if err != nil {
		//fmt.Println("Found an error in getting the args.UserID")
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	//fmt.Println("Calling Lib.Get with TargetUserID:", args.TargetUserID)
	_, err = ts.Lib.Get(args.TargetUserID)
	if err != nil {
		//fmt.Println("Found an error in getting the args.TargerUserID")
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}

	//fmt.Println("Checking if still subscribed")
	DupList,_ := ts.Lib.GetList(args.UserID+":Sub")
	//fmt.Println(DupList)
	if (len(DupList) == 0) {
	   reply.Status = tribrpc.NoSuchTargetUser
	   return nil
	   }
	for index:=0;index<len(DupList);index++ {
	    if DupList[index] == args.TargetUserID{
	       break
	       }
	     if index == len(DupList)-1 {
	     	reply.Status = tribrpc.NoSuchTargetUser
		return nil
		}
	}

	//fmt.Println("Calling RemoveFromList with UserID:", args.TargetUserID)
	ts.Lib.RemoveFromList(args.UserID+":Sub", args.TargetUserID)
	//CheckList,_ := ts.Lib.GetList(args.UserID+":Sub")
	//fmt.Println(CheckList)
	reply.Status = tribrpc.OK
	//fmt.Println("Exiting Remove Subscriptions")
	return nil
}

func (ts *tribServer) GetSubscriptions(args *tribrpc.GetSubscriptionsArgs, reply *tribrpc.GetSubscriptionsReply) error {
	//fmt.Println("Calling Get Subscriptons")
	UserCheck := ts.Clients[args.UserID]
	//fmt.Println("Checking for User info:", UserCheck)
	if UserCheck.ID == "" {
		//fmt.Println("Invalid UserID,return error")
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	//fmt.Println("Calling Lib.Get to get the subscriptions")
	SubCopy, err := ts.Lib.GetList(args.UserID + ":Sub")
	if err != nil {
		//fmt.Println("Lib.get for subs returned an err")
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	reply.Status = tribrpc.OK
	//fmt.Println(SubCopy)
	reply.UserIDs = SubCopy 
	//fmt.Println("Exiting Get Subscriptions")
	return nil

}

func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {
	//fmt.Println("Calling Get Post Tribble")
	UserCheck := ts.Clients[args.UserID]

	//fmt.Println("Checking for User info:",UserCheck)
	if UserCheck.ID == "" {
		//fmt.Println("Invalid UserID,return error")
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	TimeNow := time.Now()
	//fmt.Println("TimeNow:",TimeNow)
	TimeUnix := TimeNow.UnixNano() //This doesnt do what we think it does
	//fmt.Println("TimeUnix:",TimeUnix)
	//fmt.Println("Reverse Test:",time.Unix(TimeUnix/1000,TimeUnix%1000))
	TimeString := strconv.FormatInt(TimeUnix, 10)
	//fmt.Println("TimeString:",TimeString)
	TribString := args.UserID + ":" + TimeString
	//fmt.Println("TribString:",TribString)
	reply.Status = tribrpc.OK
	//fmt.Println("Added add and putting to Tribserver")
	ts.Lib.Put(TribString, args.Contents)
	//fmt.Println("TimeString:",TimeString)
	ts.Lib.AppendToList(args.UserID+":"+"TimeStamps", TimeString)
	//TribTest,_ := ts.Lib.GetList(args.UserID+":"+"TimeStamps")
	//fmt.Println("TribTest:",TribTest)
	//fmt.Println("Exiting Post Pribble")
	return nil
}

func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	//fmt.Println("Entering Get Tribbles")
	UserCheck := ts.Clients[args.UserID]
	//fmt.Println(UserCheck)
	if UserCheck.ID == "" {
		fmt.Println("Failed looking for a user in ts.Clients")
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	//fmt.Println("Calling GetList")
	LibList, err := ts.Lib.GetList(args.UserID + ":" + "TimeStamps") //Expect a list of timestamps
	//fmt.Println("LibList:",LibList)
	if err != nil {
	        fmt.Println("Calling GetList failed")
		reply.Status = tribrpc.OK //Changed to allow it to pass zero tribs
		return nil
	}

	TimeInt := make([]int64, len(LibList))
	//fmt.Println("Sorting Tribles by time")
	for index := 0; index < len(LibList); index++ {
		TimeInt[index], err = strconv.ParseInt(LibList[index], 10, 64)
		if err != nil {
		        fmt.Println("Ran into an error in time checking loop")
			reply.Status = tribrpc.NoSuchUser
			return nil
		}
	}
	//fmt.Println("TimeInt:",TimeInt)
	LongArrayTest := new(LongArray)
	LongArrayTest.Array = TimeInt
	sort.Sort(LongArrayTest)
	TimeInt = LongArrayTest.Array
	//fmt.Println("TimeInt Sorted:",TimeInt)
	var loopTarget int
	if 100 >= len(LibList) {
		loopTarget = len(LibList)
		fmt.Println("LoopTarget:",loopTarget)
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
	}else{
		loopTarget = 100
		//fmt.Println("Len over thus LoopTager = 100")
		TribList := make([]tribrpc.Tribble,loopTarget)
		for index := 0; index < 100; index++ {
		    Trib := new(tribrpc.Tribble)
		    Trib.UserID = args.UserID
		    ModPosition := len(LibList)-index-1
		    ModPosition = index+(len(LibList)-100)
		    Trib.Contents,_ = ts.Lib.Get(args.UserID+":"+strconv.FormatInt(TimeInt[ModPosition],10))
		    Trib.Posted = time.Unix(TimeInt[index]/1000,TimeInt[index]%1000)
		    TribList[99-index] = *Trib
		    }
		TribListTest := make([]tribrpc.Tribble,loopTarget)
		for index := 0; index < 100; index++ {
		    //TribListTest[index] = TribList[99-index]
		    TribListTest[index] = TribList[index]
		    }
		    
		reply.Status = tribrpc.OK
		reply.Tribbles = TribListTest		    
	}
	fmt.Println("Exiting Get Tribbles")
	return nil
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	UserCheck := ts.Clients[args.UserID]
	if UserCheck.ID == "" {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	UserLibList, _ := ts.Lib.GetList(args.UserID + ":" + "TimeStamps") //Expect a list of timestamps (might want to include sub))
	SubList, err := ts.Lib.GetList(args.UserID + ":Sub")
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	FullList := UserLibList
	for index := 0; index < len(SubList); index++ {
		SubLibList, errLoop := ts.Lib.GetList(SubList[index] + ":" + "TimeStamps")
		if errLoop != nil {
			reply.Status = tribrpc.NoSuchTargetUser
			return nil
		}
		TempList := make([]string, len(UserLibList)+len(SubLibList))
		for index := 0; index < len(FullList); index++ {
			TempList[index] = FullList[index]
		}
		for index := len(UserLibList); index < len(TempList); index++ {
			TempList[index] = SubLibList[index-len(UserLibList)]
		}
		FullList = TempList
	}

	TimeInt := make([]int64, len(UserLibList))

	for index := 0; index < len(FullList); index++ {
		TimeInt[index], _ = strconv.ParseInt(FullList[index], 10, 64)
	}

	LongArrayTest := new(LongArray)
	LongArrayTest.Array = TimeInt
	sort.Sort(LongArrayTest)
	TimeInt = LongArrayTest.Array

	var loopTarget int
	if 100 > len(UserLibList) {
		loopTarget = len(UserLibList)
	} else {
		loopTarget = 100
	}
	TribList := make([]tribrpc.Tribble, loopTarget)
	for index := 0; index < loopTarget; index++ {
		Trib := new(tribrpc.Tribble)
		Trib.UserID = args.UserID
		Trib.Contents, _ = ts.Lib.Get(args.UserID + ":" + strconv.FormatInt(TimeInt[index], 10))
		Trib.Posted = time.Unix(TimeInt[index], 0)
		TribList[index] = *Trib
	}
	reply.Status = tribrpc.OK
	reply.Tribbles = TribList
	return nil
}
