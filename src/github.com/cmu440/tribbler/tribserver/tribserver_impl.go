package tribserver

import (
	"Time"
	"net"
	"net/rpc"
	"sort"
	"strconv"

	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/tribrpc"
	"github.com/cmu440/tribbler/tribclient"
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

	ts.Clients = make(map[tribclient.tribClient]tribbleUser)
	ts.Lib = libstore.NewLibstore(masterServerHostPort, myHostPort, libstore.Normal)

	err = rpc.RegisterName("TribServer", tribrpc.Wrap(tribServer))
	if err != nil {
		return nil, err
	}

	rpc.HandleHTTP()
	go httpe.Serve(listener, nil)

	return ts, nil
}

func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {
	//What do we do with reply?
	_, err := libstore.Get(args.UserID) //Want to set that this err should be tribrps.Exists
	if err != nil {
		reply.Status = tribrpc.Exists
		return err
	}

	User := new(tribbleUser)
	User.ID = args.UserID

	_, err := Put(args.UserID, 0) //Make sure type is right
	ts.Clients[User.ID] = User
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	_, err := libstore.Get(args.UserID)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return err
	}

	_, err := libstore.Get(args.TargetUserID)
	if err != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return err
	}

	libstore.AppendToList(args.UserID+":Sub", args.TargetUserID)
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	_, err := libstore.Get(args.UserID)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return err
	}

	_, err := libstore.Get(args.TargetUserID)
	if err != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return err
	}

	libstore.RemoveFromList(args.UserID+":Sub", args.TargetUserID)
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetSubscriptions(args *tribrpc.GetSubscriptionsArgs, reply *tribrpc.GetSubscriptionsReply) error {

	if ts.Clients[args.UserID] == nil {
		reply.Status = tribrpc.NoSuchUser
		return error.new("Not yet a User")
	}

	SubCopy, err := libstore.Get(args.UserID + ":Sub")
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return err
	}

	reply.Status = tribrpc.OK
	reply.UserIDs = SubCopy
	return nil

}

func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {

	if ts.Clients[args.UserID] == nil {
		reply.Status = tribrpc.NoSuchUser
		return error.new("Not yet a User")
	}

	TimeNow := Time.Now()
	TimeUnix := TimeNow.Unix()
	TimeString, _ := strconv.ItoA(TimeUnix)
	TribString := args.UserID + ":" + TimeString
	reply.Status = tribrpc.OK
	libstore.Put(TribString, args.Contents)
	libstore.AppendToList(args.UserID+":"+"TimeStamps", TimeString)

	return nil
}

func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	if ts.Clients[args.UserID] == nil {
		reply.Status = tribrpc.NoSuchUser
		return _, error.new("Not yet a User")
	}

	LibList := libstore.GetList(args.UserID + ":" + "TimeStamps") //Expect a list of timestamps
	TimeInt := make([]uint64, len(LibList))

	for index := 0; index < len(LibList); index++ {
		TimeInt[index] = strconv.Atoi(LibList[index])
	}
	sort.Ints(TimeInt)

	loopTarget = min(100, len(LibList))
	TribList := make([loopTarget]tribrpc.Tribbles)
	for index = 0; index < loopTarger; index++ {
		Trib := new(tribrpc.Tribbles)
		Trib.UserID = args.UserID
		Trib.Contents, _ = libstore.Get(args.UserID + ":" + strconv.ItoA(TimeInt[index]))
		Trib.Posted = Time.Unix(TimeInt[index])
		TribList[index] = Trib
	}
	reply.Status = tribrpc.OK
	reply.Tribbles = TribList
	return nil
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	if ts.Clients[args.UserID] == nil {
		reply.Status = tribrpc.NoSuchUser
		return _, error.new("Not yet a User")
	}

	UserLibList, _ := libstore.GetList(args.UserID + ":" + "TimeStamps") //Expect a list of timestamps (might want to include sub))
	SubCopy, err := libstore.Get(args.UserID + ":Sub")
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return err
	}

	FullList := UserLibList
	for index := 0; index < len(SubCopy); index++ {
		SubLibList := libstore.GetList(SubCopy[index] + ":" + "TimeStamps")
		TempList := make([]string, len(UserLibList)+len(SubLibList))
		for index := 0; index < len(FullList); index++ {
			TempList[index] = FullList[index]
		}
		for index := len(UserLibList); index < len(TempList); index++ {
			TempList[index] = SubLibList[index-len(UserLibList)]
		}
		FullList = TempList
	}
	TimeInt := make([]uint64, len(LibList))

	for index := 0; index < len(FullList); index++ {
		TimeInt[index] = strconv.Atoi(FullList[index])
	}
	sort.Ints(TimeInt)

	loopTarget = min(100, len(LibList))
	TribList := make([loopTarget]tribrpc.Tribbles)
	for index = 0; index < loopTarger; index++ {
		Trib := new(tribrpc.Tribbles)
		Trib.UserID = args.UserID
		Trib.Contents, _ = libstore.Get(args.UserID + ":" + strconv.ItoA(TimeInt[index]))
		Trib.Posted = Time.Unix(TimeInt[index])
		TribList[index] = Trib
	}
	reply.Status = tribrpc.OK
	reply.Tribbles = TribList
	return nil
}
