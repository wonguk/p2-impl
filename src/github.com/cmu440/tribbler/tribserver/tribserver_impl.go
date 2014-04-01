package tribserver

import (
	"time"
	"net"
	"net/rpc"
	"net/http"
	"sort"
	"strconv"
	"errors"
	"strings"
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

// NewTribServer creates, starts and returns a new TribServer. masterServerHostPort
// is the master storage server's host:port and port is this port number on which
// the TribServer should listen. A non-nil error should be returned if the TribServer
// could not be started.
//
// For hints on how to properly setup RPC, see the rpc/tribrpc package.
func NewTribServer(masterServerHostPort, myHostPort string) (TribServer, error) {

	ts := new(tribServer)

	listener, err := net.Listen("tcp", myHostPort)
	if err != nil {
		return nil, err
	}

	ts.Clients = make(map[string]tribbleUser)
	ts.Lib,err = libstore.NewLibstore(masterServerHostPort, myHostPort, libstore.Normal)

	if err != nil {
		return nil, err
	}

	err = rpc.RegisterName("TribServer", tribrpc.Wrap(ts))
	if err != nil {
		return nil, err
	}

	rpc.HandleHTTP()
	go http.Serve(listener, nil)

	return ts, nil
}

func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {
	//What do we do with reply?

	_, err := ts.Lib.Get(args.UserID) //Want to set that this err should be tribrps.Exists
	if err != nil {
		reply.Status = tribrpc.Exists
		return err
	}

	User := new(tribbleUser)
	User.ID = args.UserID

	err = ts.Lib.Put(args.UserID, "0") //Make sure type is right
	ts.Clients[User.ID] = *User
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	_, err := ts.Lib.Get(args.UserID)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return err
	}

	_, err = ts.Lib.Get(args.UserID)
	if err != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return err
	}

	ts.Lib.AppendToList(args.UserID+":Sub", args.TargetUserID)
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	_, err := ts.Lib.Get(args.UserID)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return err
	}

	_, err = ts.Lib.Get(args.TargetUserID)
	if err != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return err
	}

	ts.Lib.RemoveFromList(args.UserID+":Sub", args.TargetUserID)
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetSubscriptions(args *tribrpc.GetSubscriptionsArgs, reply *tribrpc.GetSubscriptionsReply) error {
	UserCheck := ts.Clients[args.UserID]
	if (UserCheck.ID == "") {
		reply.Status = tribrpc.NoSuchUser
		return errors.New("Not yet a User")
	}

	SubCopy, err := ts.Lib.Get(args.UserID + ":Sub")
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return err
	}

	reply.Status = tribrpc.OK
	reply.UserIDs = strings.Split(SubCopy,":") //Split the string into an array based on :
	return nil

}

func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {

	UserCheck := ts.Clients[args.UserID]
	if (UserCheck.ID == "") {
		reply.Status = tribrpc.NoSuchUser
		return errors.New("Not yet a User")
	}

	TimeNow := time.Now()
	TimeUnix := TimeNow.Unix()
	TimeString := strconv.Itoa(TimeUnix)
	TribString := args.UserID + ":" + TimeString
	reply.Status = tribrpc.OK
	ts.Lib.Put(TribString, args.Contents)
	ts.Lib.AppendToList(args.UserID+":"+"TimeStamps", TimeString)

	return nil
}

func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	
	UserCheck := ts.Clients[args.UserID]
	if (UserCheck.ID == "") {
		reply.Status = tribrpc.NoSuchUser
		return errors.New("Not yet a User")
	}

	LibList,err := ts.Lib.GetList(args.UserID + ":" + "TimeStamps") //Expect a list of timestamps
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return err
	}

	TimeInt := make([]uint64, len(LibList))

	for index := 0; index < len(LibList); index++ {
		TimeInt[index],err = strconv.Atoi(LibList[index])
		if err != nil {
			reply.Status = tribrpc.NoSuchUser
			return err
		}
	}
	sort.Ints(TimeInt)
	var loopTarget int 
	if (100 > len(LibList)){
		loopTarget = len(LibList)
	} else {
		loopTarget = 100
	}
	TribList := make([]tribrpc.Tribble,loopTarget)
	for index := 0; index < loopTarget; index++ {
		Trib := new(tribrpc.Tribble)
		Trib.UserID = args.UserID
		Trib.Contents, _ = ts.Lib.Get(args.UserID + ":" + strconv.Itoa(TimeInt[index]))
		Trib.Posted = time.Unix(TimeInt[index],0)
		TribList[index] = *Trib
	}
	reply.Status = tribrpc.OK
	reply.Tribbles = TribList
	return nil
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	UserCheck := ts.Clients[args.UserID]
	if (UserCheck.ID == "") {
		reply.Status = tribrpc.NoSuchUser
		return errors.New("Not yet a User")
	}

	UserLibList, _ := ts.Lib.GetList(args.UserID + ":" + "TimeStamps") //Expect a list of timestamps (might want to include sub))
	SubList, err := ts.Lib.GetList(args.UserID + ":Sub")
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return err
	}

	FullList := UserLibList
	for index := 0; index < len(SubList); index++ {
		SubLibList,errLoop := ts.Lib.GetList(SubList[index] + ":" + "TimeStamps")
		if errLoop != nil {
			reply.Status = tribrpc.NoSuchTargetUser  
			return errLoop
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
	
	TimeInt := make([]uint64, len(UserLibList))

	for index := 0; index < len(FullList); index++ {
		TimeInt[index],err = strconv.Atoi(FullList[index])
		if err != nil {
			return err
		}
	}
	sort.Ints(TimeInt)

	var loopTarget int 
	if (100 > len(UserLibList)){
		loopTarget = len(UserLibList)
	} else {
		loopTarget = 100
	}
	TribList := make([]tribrpc.Tribble,loopTarget)
	for index := 0; index < loopTarget; index++ {
		Trib := new(tribrpc.Tribble)
		Trib.UserID = args.UserID
		Trib.Contents, _ = ts.Lib.Get(args.UserID + ":" + strconv.Itoa(TimeInt[index]))
		Trib.Posted = time.Unix(TimeInt[index],0)
		TribList[index] = *Trib
	}
	reply.Status = tribrpc.OK
	reply.Tribbles = TribList
	return nil
}
