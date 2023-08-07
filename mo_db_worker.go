package mongostorage

import (
	"context"
	"fmt"
	"github.com/go-while/go-utils"
	"go.mongodb.org/mongo-driver/mongo"
	"log"
	"time"
)

// ExtendContextTimeout extends the deadline of a given context by canceling the previous
// function written by AI.
func ExtendContextTimeout(ctx context.Context, cancel context.CancelFunc, MongoTimeout int64) (context.Context, context.CancelFunc) {
	//logf(DEBUG, "ExtendContextTimeout")
	cancel()
	newTimeout := time.Second * time.Duration(MongoTimeout)
	//deadline := time.Now().Add(newTimeout)
	ctx, cancel = context.WithTimeout(context.Background(), newTimeout)
	//ctx, cancel = context.WithDeadline(context.Background(), deadline)
	return ctx, cancel
} // end func ExtendContextTimeout

// requeue_Articles requeues a slice of articles into the MongoDB insert queue.
func requeue_Articles(articles []*MongoArticle) {
	logf(DEBUG, "Warn requeue_Articles: articles=%d", len(articles))

	for _, article := range articles {
		Mongo_Insert_queue <- article
	}
} // end func requeue_Articles

// mongoWorker_Insert is a goroutine function responsible for processing incoming insert requests from the 'Mongo_Insert_queue'.
// It inserts articles into a MongoDB collection based on the articles provided in the 'Mongo_Insert_queue'.
// The function continuously listens for insert requests and processes them until the 'Mongo_Insert_queue' is closed.
// It accumulates the articles to be inserted until the number of articles reaches the limit set by cfg.InsBatch.
// Once the 'Mongo_Insert_queue' is closed, the function performs disconnection from the MongoDB server and terminates.
//
// If the 'TestAfterInsert' flag is set to true, the function will perform article existence checks after each insertion.
// The check logs the results of the existence test for each article.
// function partly written by AI.
func mongoWorker_Insert(wid int, wType *string, cfg *MongoStorageConfig) {
	if wid <= 0 {
		log.Printf("Error mongoWorker_Insert wid <= 0")
		return
	}
	did, bad := 0, 0
	updateWorkerStatus(wType, update{Boot: true})
	defer updateWorkerStatus(wType, update{Stop: true})
	reboot := false
	who := fmt.Sprintf("mongoWorker_Insert#%d", wid)
	logf(DEBUG, "++ Start %s", who)
	var ctx context.Context
	var cancel context.CancelFunc
	var client *mongo.Client
	var collection *mongo.Collection
	var err error
	attempts := 0
	for {
		ctx, cancel, client, collection, err = ConnectMongoDB(who, cfg)
		if err != nil {
			attempts++
			log.Printf("Error %s ConnectMongoDB err='%v'", who, err)
			time.Sleep(time.Second * calculateExponentialBackoff(attempts))
			continue
		}
		break
	}
	timeout := time.After(time.Millisecond * time.Duration(cfg.FlushTimer))
	articles := []*MongoArticle{}
	is_timeout := false
	var diff int64
	var last_insert int64
	stop := false
forever:
	for {
		do_insert := false
		len_arts := len(articles)
		if len_arts == cfg.InsBatch || is_timeout {
			diff = utils.UnixTimeMilliSec() - last_insert

			if len_arts >= cfg.InsBatch {
				do_insert = true
			} else if is_timeout && len_arts > 0 && diff > cfg.FlushTimer {
				do_insert = true
			}
			if is_timeout {
				is_timeout = false
			}

			if do_insert {
				logf(DEBUG, "%s Pre-Ins Many msgidhashes=%d", who, len_arts)
				did += len_arts
				ctx, cancel = ExtendContextTimeout(ctx, cancel, cfg.MongoTimeout)
				if err := InsertManyArticles(ctx, collection, articles); err != nil {
					// connection error
					go requeue_Articles(articles)
					articles = []*MongoArticle{}
					reboot = true
				}
				if cfg.TestAfterInsert {
					for _, article := range articles {
						ctx, cancel = ExtendContextTimeout(ctx, cancel, cfg.MongoTimeout)
						if retbool, err := CheckIfArticleExistsByMessageIDHash(ctx, collection, article.MessageIDHash); retbool {
							// The article with the given hash exists.
							logf(DEBUG, "%s article exists: %s", who, *article.MessageIDHash)
						} else if err != nil {
							log.Printf("Error %s CheckIfArticleExistsByMessageIDHash: %s err %v", who, *article.MessageIDHash, err)
						}
					}
				}
				last_insert = utils.UnixTimeMilliSec()
				articles = []*MongoArticle{}
			}
		}
		if stop {
			break forever
		}
	insert_queue:
		select {
		case article, ok := <-Mongo_Insert_queue:
			if !ok {
				logf(DEBUG, "__ Quit %s", who)
				break forever
			}
			//DEBUGSLEEP()
			//logf(DEBUG, "%s process Mongo_Insert_queue", who) // spammy
			articles = append(articles, article)
			if len(articles) >= cfg.InsBatch {
				break insert_queue
			}
		case <-timeout:
			is_timeout = true
			if did > 0 || bad > 0 {
				updateWorkerStatus(wType, update{Did: did, Bad: bad})
				did, bad = 0, 0
			}
			maxID := iStop_Worker(wType)
			if wid > maxID {
				logf(DEBUG, "-- Stopping %s", who)
				stop = true
				continue forever
			}
			timeout = newFlushTimer(cfg)
			//logf(DEBUG, "mongoWorker_Insert alive hashs=%d", len(msgidhashes))
		} // end select insert_queue
	} // end for forever
	if len(articles) > 0 {
		if err := InsertManyArticles(ctx, collection, articles); err != nil {
			// connection error
			go requeue_Articles(articles)
			articles = []*MongoArticle{}
			reboot = true
		}
		did += len(articles)
	}
	DisConnectMongoDB(&who, ctx, client)
	updateWorkerStatus(wType, update{Did: did, Bad: bad})
	logf(DEBUG, "xx End %s stop=%t reboot=%t", who, stop, reboot)
	if reboot {
		go mongoWorker_Insert(wid, wType, cfg)
	} else {
		queued := len(Mongo_Insert_queue)
		if queued > 0 {
			logf(DEBUG, "WARN Mongo_Insert_queue queued=%d", queued)
		}
	}
} // end func mongoWorker_Insert

// mongoWorker_Delete is a goroutine function responsible for processing incoming delete requests from the 'Mongo_Delete_queue'.
// It deletes articles from a MongoDB collection based on the given MessageIDHashes provided in the 'Mongo_Delete_queue'.
// The function continuously listens for delete requests and processes them until the 'Mongo_Delete_queue' is closed.
// Note: If cfg.DelBatch is set to 1, the function will delete articles one by one, processing individual delete requests from the queue.
// Otherwise, it will delete articles in bulk based on the accumulated MessageIDHashes.
// function partly written by AI.
func mongoWorker_Delete(wid int, wType *string, cfg *MongoStorageConfig) {
	if wid <= 0 {
		log.Printf("Error mongoWorker_Delete wid <= 0")
		return
	}
	did, bad := 0, 0
	updateWorkerStatus(wType, update{Boot: true})
	defer updateWorkerStatus(wType, update{Stop: true})
	who := fmt.Sprintf("mongoWorker_Delete#%d", wid)
	logf(DEBUG, "++ Start %s", who)
	var ctx context.Context
	var cancel context.CancelFunc
	var client *mongo.Client
	var collection *mongo.Collection
	var err error
	attempts := 0
	for {
		ctx, cancel, client, collection, err = ConnectMongoDB(who, cfg)
		if err != nil {
			attempts++
			log.Printf("Error %s ConnectMongoDB err='%v'", who, err)
			time.Sleep(time.Second * calculateExponentialBackoff(attempts))
			continue
		}
		break
	}
	timeout := time.After(time.Millisecond * time.Duration(cfg.FlushTimer))
	//msgidhashes := []*string{}
	delrequests := []*MongoDelRequest{}
	delnoretchan := []*string{} // contains only (batched *messageidhash) if no return chan is set
	is_timeout := false
	var diff int64
	var last_delete int64
	reboot := false
	stop := false
forever:
	for {
		len_hashs := len(delrequests) + len(delnoretchan)
		do_delete := false
		if len_hashs == cfg.DelBatch || is_timeout {
			diff = utils.UnixTimeMilliSec() - last_delete
			if len_hashs >= cfg.DelBatch {
				do_delete = true
			} else if is_timeout && len_hashs > 0 && diff > cfg.FlushTimer {
				do_delete = true
			}
			if is_timeout {
				is_timeout = false
			}
		} // check if do_delete

		if do_delete {
			logf(DEBUG, "%s Pre-Del Many len_hashs=%d delrequests=%d delnoretchan=%d cfg.DelBatch=%d", who, len_hashs, len(delrequests), len(delnoretchan), cfg.DelBatch)

			// process requests without RetChan first
			if len(delnoretchan) > 0 {
				ctx, cancel = ExtendContextTimeout(ctx, cancel, cfg.MongoTimeout)
				//log.Print("::before DeleteManyArticles")
				_, err := DeleteManyArticles(ctx, collection, delnoretchan)
				//log.Print("::after DeleteManyArticles")
				if err != nil {
					bad++
					// connection error
					reboot = true
				}
				did += len(delnoretchan)
				delnoretchan = []*string{}
			}

			// process requests with RetChan
			if len(delrequests) > 0 {
				for _, delreq := range delrequests {
					ctx, cancel = ExtendContextTimeout(ctx, cancel, cfg.MongoTimeout)
					deleted, err := DeleteManyArticles(ctx, collection, delreq.Msgidhashes)
					if err != nil {
						bad++
						// connection error
						reboot = true
					}
					if delreq.RetChan != nil {
						logf(DEBUG, "return delete response to delreq.RetChan")
						delreq.RetChan <- deleted
					}
					did++
				}
				delrequests = []*MongoDelRequest{}
			}
			last_delete = utils.UnixTimeMilliSec()
		} else {
			//logf(DEBUG, "!do_delete len_hashs=%d is_timeout=%t last=%d", len_hashs, is_timeout, utils.UnixTimeSec() - last_insert)
		}
		if stop {
			break forever
		}
	select_delete_queue:
		select {
		case delreq, ok := <-Mongo_Delete_queue:
			if !ok {
				logf(DEBUG, "__ Quit %s", who)
				break forever
			}
			//DEBUGSLEEP()
			//logf(DEBUG, "%s process Mongo_Delete_queue", who)  // spammy
			if len(delreq.Msgidhashes) == 0 {
				logf(DEBUG, "WARN Mongo_Delete_queue got empty request")
				continue // the select
			}
			if delreq.RetChan == nil { // requests without return merge into delnoretchan
				for _, msgidhash := range delreq.Msgidhashes {
					delnoretchan = append(delnoretchan, msgidhash)
				}
			} else {
				delrequests = append(delrequests, delreq)
				if len(delrequests) >= cfg.DelBatch {
					break select_delete_queue
				}
			}
			if len(delrequests)+len(delnoretchan) >= cfg.DelBatch {
				break select_delete_queue
			}
		case <-timeout:
			is_timeout = true
			if did > 0 || bad > 0 {
				updateWorkerStatus(wType, update{Did: did, Bad: bad})
				did, bad = 0, 0
			}
			maxID := iStop_Worker(wType)
			if wid > maxID {
				logf(DEBUG, "-- Stopping %s", who)
				stop = true
				continue forever
			}
			timeout = newFlushTimer(cfg)
			logf(DEBUG, "%s alive delrequests=%d delnoretchan=%d", who, len(delrequests), len(delnoretchan))
			//break select_delete_queue
		} // end select delete_queue
	} // end for forever
	DisConnectMongoDB(&who, ctx, client)
	updateWorkerStatus(wType, update{Did: did, Bad: bad})
	logf(DEBUG, "xx End %s stop=%t reboot=%t", who, stop, reboot)
	if reboot {
		go mongoWorker_Delete(wid, wType, cfg)
	} else {
		queued := len(Mongo_Delete_queue)
		if queued > 0 {
			logf(DEBUG, "WARN Mongo_Delete_queue queued=%d", queued)
		}
	}
} // end func mongoWorker_Delete

// mongoWorker_Reader is a goroutine function responsible for processing incoming read requests from the 'Mongo_Reader_queue'.
// It reads articles from a MongoDB collection based on the given MessageIDHashes provided in the 'getreq' parameter.
// The function continuously listens for read requests and processes them until the 'Mongo_Reader_queue' is closed.
// Once the 'Mongo_Reader_queue' is closed, the function performs disconnection from the MongoDB server and terminates.
// function partly written by AI.
func mongoWorker_Reader(wid int, wType *string, cfg *MongoStorageConfig) {
	if wid <= 0 {
		log.Printf("Error mongoWorker_Reader wid <= 0")
		return
	}
	did, bad := 0, 0
	updateWorkerStatus(wType, update{Boot: true})
	defer updateWorkerStatus(wType, update{Stop: true})
	who := fmt.Sprintf("mongoWorker_Reader#%d", wid)
	logf(DEBUG, "++ Start %s", who)
	var ctx context.Context
	var cancel context.CancelFunc
	var client *mongo.Client
	var collection *mongo.Collection
	var err error
	attempts := 0
	for {
		ctx, cancel, client, collection, err = ConnectMongoDB(who, cfg)
		if err != nil {
			attempts++
			log.Printf("Error %s ConnectMongoDB err='%v'", who, err)
			time.Sleep(time.Second * calculateExponentialBackoff(attempts))
			continue
		}
		break
	}
	timeout := time.After(time.Millisecond * time.Duration(cfg.FlushTimer))
	reboot := false
	stop := false
	// Process incoming read requests forever.
forever:
	for {
		select {
		case getreq, ok := <-Mongo_Reader_queue:
			if !ok {
				logf(DEBUG, "__ Quit %s", who)
				break forever
			}
			//DEBUGSLEEP()
			//logf(DEBUG, "%s process Mongo_Reader_queue", who) // spammy
			did++
			if getreq.STAT {
				found := 0
				var articles []*MongoArticle
				for i, messageIDHash := range getreq.Msgidhashes {
					ctx, cancel = ExtendContextTimeout(ctx, cancel, cfg.MongoTimeout)
					retbool, err := CheckIfArticleExistsByMessageIDHash(ctx, collection, messageIDHash)
					if err != nil {
						log.Printf("Error %s STAT err='%v'", who, err)
						time.Sleep(time.Second)
						reboot = true
						Mongo_Reader_queue <- getreq // requeue
						break forever
					}
					if retbool {
						found++
					}
					articles[i] = &MongoArticle { MessageIDHash: messageIDHash, Found: retbool }
				}
				if getreq.RetChan != nil {
					//logf(DEBUG, "passing response %d/%d STAT articles to getreq.RetChan", len_got_arts, len_request)
					getreq.RetChan <- articles
					// sender does not close the getreq.RetChan here so it can be reused for next read request
				} else {
					logf(DEBUG, "WARN %s got %d/%d STAT articles getreq.RetChan=nil", who, found,len(getreq.Msgidhashes))
				}
				continue forever
			}

			ctx, cancel = ExtendContextTimeout(ctx, cancel, cfg.MongoTimeout)
			// Read articles for the given msgidhashes.
			articles, err := RetrieveArticlesByMessageIDHashes(ctx, collection, getreq.Msgidhashes)
			if err != nil {
				bad++
				log.Printf("Error %s RetrieveArticlesByMessageIDHashes hashs=%d err='%v'", who, len(getreq.Msgidhashes), err)
				reboot = true
				break forever
			}
			len_request := len(getreq.Msgidhashes)
			len_got_arts := len(articles)
			if getreq.RetChan != nil {
				//logf(DEBUG, "passing response %d/%d read articles to getreq.RetChan", len_got_arts, len_request)
				getreq.RetChan <- articles
				// sender does not close the getreq.RetChan here so it can be reused for next read request
			} else {
				logf(DEBUG, "WARN %s got %d/%d read articles getreq.RetChan=nil", who, len_got_arts, len_request)
			}
			// Do something with the articles, e.g., handle them or send them to another channel.
		case <-timeout:
			//is_timeout = true
			if did > 0 || bad > 0 {
				updateWorkerStatus(wType, update{Did: did, Bad: bad})
				did, bad = 0, 0
			}
			maxID := iStop_Worker(wType)
			if wid > maxID {
				logf(DEBUG, "-- Stopping %s", who)
				stop = true
				break forever
			}
			timeout = newFlushTimer(cfg)
			logf(DEBUG, "%s alive", who)
			//break reader_queue
		} // end select
	}
	DisConnectMongoDB(&who, ctx, client)
	updateWorkerStatus(wType, update{Did: did, Bad: bad})
	logf(DEBUG, "xx End %s stop=%t reboot=%t", who, stop, reboot)
	if reboot {
		go mongoWorker_Reader(wid, wType, cfg)
	} else {
		queued := len(Mongo_Reader_queue)
		if queued > 0 {
			logf(DEBUG, "WARN Mongo_Reader_queue queued=%d", queued)
		}
	}
} // end func mongoWorker_Reader

func newFlushTimer(cfg *MongoStorageConfig) <-chan time.Time {
	return time.After(time.Millisecond * time.Duration(cfg.FlushTimer))
}

// EOF mo_db_worker.go
