package libstore

import (
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
	deleteChan chan struct{}

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
	for {
		select {
		case req := <-cm.cacheChan:
			if c, ok := cm.cacheMap[req.key]; ok {
				c.reqChan <- req
			} else {
				req.cache <- nil
			}
		case newCache := <-cm.newCacheChan:
			cm.cacheMap[newCache.key] = newCache
			go newCache.cacheHandler(cm.deleteCacheChan)
		case key := <-cm.deleteCacheChan:
			del, ok := cm.cacheMap[key]
			if ok {
				close(del.delChan)
			}
			delete(cm.cacheMap, key)
		case rev := <-cm.revokeCacheChan:
			c, ok := cm.cacheMap[rev.key]

			if ok {
				rev.ok <- true
				close(c.delChan)
			} else {
				rev.ok <- false
			}
		}
	}
}

func (ch *cacheCell) cacheHandler(delCacheChan chan string) {
	duration := time.Duration(ch.duration) * time.Second
	epoch := time.NewTimer(duration)

	cache := cache{
		data: ch.value, listData: ch.listValue,
	}

	ch.valid = true

	for {
		select {
		case <-epoch.C:
			delCacheChan <- ch.key
			ch.valid = false
		case <-ch.delChan:
			return
		case req := <-ch.reqChan:
			if ch.valid {
				req.cache <- &cache
			} else {
				req.cache <- nil
			}
		}
	}
}

func (qm *queryMaster) startQueryMaster() {
	for {
		select {
		case req := <-qm.queryChan:
			query, ok := qm.queryMap[req.key]
			if !ok {
				query = new(queryCell)

				qc := make(chan *queryRequest)
				dc := make(chan struct{})
				query.queryChan = qc
				query.deleteChan = dc
				query.key = req.key

				qm.queryMap[req.key] = query

				go query.queryHandler(qm.deleteChan)
			}

			query.queryChan <- req
		case key := <-qm.deleteChan:
			q, ok := qm.queryMap[key]

			if ok {
				close(q.deleteChan)
			}

			delete(qm.queryMap, key)
		}
	}
}

func (qc *queryCell) queryHandler(del chan string) {
	duration := time.Duration(storagerpc.QueryCacheSeconds) * time.Second
	epoch := time.NewTimer(duration)

	for {
		select {
		case <-epoch.C:
			del <- qc.key
		case req := <-qc.queryChan:
			qc.count++
			if qc.count >= storagerpc.QueryCacheThresh {
				req.lease <- true

				del <- qc.key
			} else {
				req.lease <- false
			}
		case <-qc.deleteChan:
			return
		}
	}
}
