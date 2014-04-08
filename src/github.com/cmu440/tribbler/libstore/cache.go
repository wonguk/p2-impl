package libstore

import (
	"container/list"
	"time"

	"github.com/cmu440/tribbler/rpc/storagerpc"
)

type valueType bool

const (
	String     valueType = true
	StringList valueType = false
)

type cacheCell struct {
	valueType valueType
	key       string
	value     string
	listValue []string
	duration  int
	valid     bool

	reqChan chan *cacheRequest
	delChan chan struct{}
}

type cacheMaster struct {
	cacheMap        map[string]*cacheCell
	cacheChan       chan *cacheRequest
	newCacheChan    chan *cacheCell
	deleteCacheChan chan string
	revokeCacheChan chan *revokeRequest
}

type cache struct {
	data     string
	listData []string
}

type queryCell struct {
	key        string
	queryChan  chan *queryRequest
	deleteChan chan bool
	times      *list.List

	duration int
	count    int
}

type queryMaster struct {
	queryMap map[string]*queryCell

	queryChan  chan *queryRequest
	deleteChan chan string
}

type cacheRequest struct {
	key   string
	cache chan *cache
}

type queryRequest struct {
	key   string
	lease chan bool
}

type revokeRequest struct {
	key string
	ok  chan bool
}

// TODO QueryMaster: Keeps track of the different queries made
//                 - map[key]QueryHandler
//                 - spawns a queryhandler if new query made
//                 - sends signal to relevant queryhandler
// TODO QueryHandler: Keeps track of number of times query has been requested
//                  - If query requested enough times in given timeslot, spawn

// TODO CacheMaster: handles request for cache cell
//                 - Keeps track of the different CacheHandlers
//                 - CheckCache -> If Exists, Send channel to relevant cachehandler
//                                 Else, return null?

// TODO CacheHandler: For a given key, handles its cache
//                  - When timeout, delete itself
//                  - If data requested, send data
//                  - If revoked,delete itself

// Order of operations:
// 1) Query CacheMaster to check if already cached
// 2) If Cached, get data from cache
// 3) If Not Cached, tell QueryMaster
// 4) If not leaseable, return(?) fals
// 5) If leasable, return(?) true
// 6) Send request for lease
// 7) if Lease, then send it to Cache Master

func (cm *cacheMaster) startCacheMaster() {
	LOGV.Println("[LIB] CacheMaster: Starting...")
	for {
		LOGV.Println(
			"[LIB] CacheMaster: There are",
			len(cm.cacheMap),
			"cache cells")
		select {
		case req := <-cm.cacheChan:
			LOGV.Println("[LIB] CacheMaster: received cache request for ", req.key)
			if c, ok := cm.cacheMap[req.key]; ok {
				c.reqChan <- req
			} else {
				LOGV.Println("[LIB] CacheMaster: cache is not found for ", req.key)
				req.cache <- nil
			}
		case newCache := <-cm.newCacheChan:
			LOGV.Println("[LIB] CacheMaster: received new cache cell for",
				newCache.key)
			cm.cacheMap[newCache.key] = newCache
			go newCache.cacheHandler(cm.deleteCacheChan)
		case key := <-cm.deleteCacheChan:
			LOGV.Println("[LIB] CacheMaster: recieved delete request for", key)
			del, ok := cm.cacheMap[key]
			if ok {
				LOGV.Println("[LIB] CacheMaster: closing delChan for", key)
				close(del.delChan)
			}
			LOGV.Println("[LIB] CacheMaster: removing from cachemap")
			delete(cm.cacheMap, key)
		case rev := <-cm.revokeCacheChan:
			LOGV.Println("[LIB] CacheMaster: revoke request received for", rev.key)
			c, ok := cm.cacheMap[rev.key]

			if ok {
				LOGV.Println("[LIB]", "CacheMaster:", "revoking cache cell for", rev.key)
				rev.ok <- true
				close(c.delChan)
				delete(cm.cacheMap, rev.key)
			} else {
				LOGV.Println("[LIB]", "CacheMaster:", "cache cell to revoke not found")
				rev.ok <- false
			}
		}
	}
}

func (ch *cacheCell) cacheHandler(delCacheChan chan string) {
	LOGV.Println("[LIB]", "CacheCell:", "cache cell initializing....", ch.key)
	duration := time.Duration(ch.duration) * time.Second
	epoch := time.NewTimer(duration)

	cache := cache{
		data: ch.value, listData: ch.listValue,
	}

	ch.valid = true

	for {
		select {
		case <-epoch.C:
			LOGV.Println("[LIB]", "CacheCell:", "Lease Duration has ended for", ch.key)
			delCacheChan <- ch.key
			ch.valid = false
		case <-ch.delChan:
			LOGV.Println("[LIB]", "CacheCell:", "deleting cell for", ch.key)
			return
		case req := <-ch.reqChan:
			LOGV.Println("[LIB]", "CacheCell:", "request recieved for", ch.key)
			if ch.valid {
				LOGV.Println("[LIB]", "CacheCell:", "sending cache for", ch.key)
				req.cache <- &cache
			} else {
				LOGV.Println("[LIB]", "CacheCell:", "cache is invalid")
				req.cache <- nil
			}
		}
	}
}

func (qm *queryMaster) startQueryMaster() {
	LOGV.Println("[LIB]", "QueryMaster", "Starting....")
	for {
		select {
		case req := <-qm.queryChan:
			LOGV.Println("[LIB]", "QueryMaster", "request recieved for", req.key)
			query, ok := qm.queryMap[req.key]
			if !ok {
				LOGV.Println("[LIB]", "QueryMaster", "initializing query cell for", req.key)
				query = new(queryCell)

				query.queryChan = make(chan *queryRequest)
				query.deleteChan = make(chan bool)

				query.key = req.key
				query.times = list.New()

				qm.queryMap[req.key] = query

				go query.queryHandler(qm.deleteChan)
			}
			query.queryChan <- req
		case key := <-qm.deleteChan:
			LOGV.Println("[LIB]", "QueryMaster", "delete request for", key)
			q, ok := qm.queryMap[key]

			if ok {
				LOGV.Println("[LIB]", "QueryMaster", "closing delete chan for", key)
				q.deleteChan <- true
			}

			delete(qm.queryMap, key)
		}
	}
}

func (qc *queryCell) queryHandler(del chan string) {
	LOGV.Println("[LIB]", "QueryCell", "Starting...")
	duration := time.Duration(storagerpc.QueryCacheSeconds) * time.Second
	epoch := time.NewTimer(duration)

	for {
		select {
		case <-epoch.C:
			LOGV.Println("[LIB]", "QueryCell", "Query Duration reached for", qc.key)
			del <- qc.key
		case req := <-qc.queryChan:
			epoch.Reset(duration)

			cur := time.Now()
			qc.times.PushFront(cur)
			e := qc.times.Front()

			ok := true

			for i := 0; i <= storagerpc.QueryCacheThresh; i++ {
				if e == nil || cur.Sub((e.Value).(time.Time)).Seconds() > storagerpc.QueryCacheSeconds {
					req.lease <- false

					for ; e != nil; e = e.Next() {
						qc.times.Remove(e)
					}

					ok = false
					break
				}

				e = e.Next()
			}

			if ok {
				req.lease <- true
			}

		case <-qc.deleteChan:
			LOGV.Println("[LIB]", "QueryCell", "deleting querycell for", qc.key)
			return
		}
	}
}
