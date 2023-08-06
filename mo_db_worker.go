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
	//log.Printf("ExtendContextTimeout")
	cancel()
	newTimeout := time.Second * time.Duration(MongoTimeout)
	//deadline := time.Now().Add(newTimeout)
	ctx, cancel = context.WithTimeout(context.Background(), newTimeout)
	//ctx, cancel = context.WithDeadline(context.Background(), deadline)
	return ctx, cancel
} // end func ExtendContextTimeout

// requeue_Articles requeues a slice of articles into the MongoDB insert queue.
func requeue_Articles(articles []*MongoArticle) {
	log.Printf("Warn requeue_Articles: articles=%d", len(articles))

	for _, article := range articles {
		Mongo_Insert_queue <- *article
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
	log.Printf("++ Start %s", who)
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
				log.Printf("%s Pre-Ins Many msgidhashes=%d", who, len_arts)
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
							log.Printf("%s article exists: %s", who, *article.MessageIDHash)
						} else if err != nil {
							log.Printf("Error %s CheckIfArticleExistsByMessageIDHash: %s err %v", who, *article.MessageIDHash, err)
						}
					}
				}
				last_insert = utils.UnixTimeMilliSec()
				articles = []*MongoArticle{}
			}
		}
	insert_queue:
		select {
		case article, ok := <-Mongo_Insert_queue:
			if !ok {
				log.Printf("__ Quit %s", who)
				break forever
			}
			// DEBUG time.Sleep(time.Second)
			//log.Printf("%s process Mongo_Insert_queue", who)
			articles = append(articles, &article)
			if len(articles) >= cfg.InsBatch {
				break insert_queue
			}
		case <-timeout:
			is_timeout = true
			if did > 0 || bad > 0 {
				updateWorkerStatus(wType, update{Did: did, Bad: bad})
				did, bad = 0, 0
			}
			maxID := iStop_Worker(*wType)
			if wid > maxID {
				log.Printf("-- Stopping %s", who)
				break forever // stop Worker
			}
			timeout = newFlushTimer(cfg)
			//log.Printf("mongoWorker_Insert alive hashs=%d", len(msgidhashes))
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
	DisConnectMongoDB(who, ctx, client)
	updateWorkerStatus(wType, update{Did: did, Bad: bad})
	log.Printf("xx End %s reboot=%t", who, reboot)
	if reboot {
		go mongoWorker_Insert(wid, wType, cfg)
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
	log.Printf("++ Start %s", who)
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
	msgidhashes := []*string{}
	is_timeout := false
	var diff int64
	var last_delete int64
forever:
	for {
		len_hashs := len(msgidhashes)
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
			log.Printf("%s Pre-Del Many msgidhashes=%d", who, len_hashs)
			ctx, cancel = ExtendContextTimeout(ctx, cancel, cfg.MongoTimeout)
			DeleteManyArticles(ctx, collection, msgidhashes) // TODO catch error !
			msgidhashes = []*string{}
			last_delete = utils.UnixTimeMilliSec()
			did += len(msgidhashes)
		} else {
			//log.Printf("!do_delete len_hashs=%d is_timeout=%t last=%d", len_hashs, is_timeout, utils.UnixTimeSec() - last_insert)
		}
	select_delete_queue:
		select {
		case msgidhash, ok := <-Mongo_Delete_queue:
			if !ok {
				log.Printf("__ Quit %s", who)
				break forever
			}
			did++
			// DEBUG time.Sleep(time.Second)
			//log.Printf("%s process Mongo_Delete_queue", who)
			if cfg.DelBatch == 1 { // deletes articles one by one
				log.Printf("%s Pre-Del One msgidhash='%s'", who, msgidhash)
				ctx, cancel = ExtendContextTimeout(ctx, cancel, cfg.MongoTimeout)
				err := DeleteArticlesByMessageIDHash(ctx, collection, &msgidhash)
				if err != nil {
					log.Printf("Error deleting messageIDHash=%s err='%v'", msgidhash, err)
					bad++
					continue
				}
				log.Printf("%s Deleted messageIDHash=%s", who, msgidhash)
			} else {
				msgidhashes = append(msgidhashes, &msgidhash)
				//log.Printf("Append Del Worker: msgidhash='%s' to msgidhashes=%d", msgidhash, len(msgidhashes))
			}
			if len(msgidhashes) >= cfg.DelBatch {
				break select_delete_queue
			}
		case <-timeout:
			is_timeout = true
			if did > 0 || bad > 0 {
				updateWorkerStatus(wType, update{Did: did, Bad: bad})
				did, bad = 0, 0
			}
			maxID := iStop_Worker(*wType)
			if wid > maxID {
				log.Printf("-- Stopping %s", who)
				break forever // stop Worker
			}
			timeout = newFlushTimer(cfg)
			//log.Printf("mongoWorker_Delete alive hashs=%d", len(msgidhashes))
			//break select_delete_queue
		} // end select delete_queue
	} // end for forever
	if len(msgidhashes) > 0 {
		DeleteManyArticles(ctx, collection, msgidhashes) // TODO: catch error !
	}
	DisConnectMongoDB(who, ctx, client)
	updateWorkerStatus(wType, update{Did: did, Bad: bad})
	log.Printf("xx End %s", who)
} // end func mongoWorker_Delete

func newFlushTimer(cfg *MongoStorageConfig) <-chan time.Time {
	return time.After(time.Millisecond * time.Duration(cfg.FlushTimer))
}

// mongoWorker_Reader is a goroutine function responsible for processing incoming read requests from the 'Mongo_Reader_queue'.
// It reads articles from a MongoDB collection based on the given MessageIDHashes provided in the 'readreq' parameter.
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
	log.Printf("++ Start %s", who)
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
	// Process incoming read requests forever.
forever:
	for {
		select {
		case readreq, ok := <-Mongo_Reader_queue:
			if !ok {
				log.Printf("__ Quit %s", who)
				break forever
			}
			// DEBUG time.Sleep(time.Second)
			//log.Printf("%s process Mongo_Reader_queue", who)
			did++
			if readreq.STAT {
				found := 0
				var articles []*MongoArticle
				for i, messageIDHash := range readreq.Msgidhashes {
					ctx, cancel = ExtendContextTimeout(ctx, cancel, cfg.MongoTimeout)
					retbool, err := CheckIfArticleExistsByMessageIDHash(ctx, collection, messageIDHash)
					if err != nil {
						log.Printf("Error %s STAT err='%v'", who, err)
						time.Sleep(time.Second)
						reboot = true
						Mongo_Reader_queue <- readreq // requeue
						break forever
					}
					if retbool {
						found++
					}
					articles[i] = &MongoArticle { MessageIDHash: messageIDHash, Found: retbool }
				}
				if readreq.RetChan != nil {
					//log.Printf("passing response %d/%d STAT articles to readreq.RetChan", len_got_arts, len_request)
					readreq.RetChan <- articles
					// sender does not close the readreq.RetChan here so it can be reused for next read request
				} else {
					log.Printf("WARN %s got %d/%d STAT articles readreq.RetChan=nil", who, found,len(readreq.Msgidhashes))
				}
				continue forever
			}

			ctx, cancel = ExtendContextTimeout(ctx, cancel, cfg.MongoTimeout)
			// Read articles for the given msgidhashes.
			articles, err := RetrieveArticlesByMessageIDHashes(ctx, collection, readreq.Msgidhashes)
			if err != nil {
				bad++
				log.Printf("Error %s RetrieveArticlesByMessageIDHashes hashs=%d err='%v'", who, len(readreq.Msgidhashes), err)
				reboot = true
				break forever
			}
			len_request := len(readreq.Msgidhashes)
			len_got_arts := len(articles)
			if readreq.RetChan != nil {
				//log.Printf("passing response %d/%d read articles to readreq.RetChan", len_got_arts, len_request)
				readreq.RetChan <- articles
				// sender does not close the readreq.RetChan here so it can be reused for next read request
			} else {
				log.Printf("WARN %s got %d/%d read articles readreq.RetChan=nil", who, len_got_arts, len_request)
			}
			// Do something with the articles, e.g., handle them or send them to another channel.
		case <-timeout:
			//is_timeout = true
			if did > 0 || bad > 0 {
				updateWorkerStatus(wType, update{Did: did, Bad: bad})
				did, bad = 0, 0
			}
			maxID := iStop_Worker("reader")
			if wid > maxID {
				log.Printf("-- Stopping %s", who)
				break forever
			}
			timeout = newFlushTimer(cfg)
			//log.Printf("mongoWorker_Reader alive hashs=%d", len(msgidhashes))
			//break reader_queue
		} // end select
	}
	DisConnectMongoDB(who, ctx, client)
	updateWorkerStatus(wType, update{Did: did, Bad: bad})
	log.Printf("xx End %s", who)
	if reboot {
		go mongoWorker_Reader(wid, wType, cfg)
	}
} // end func mongoWorker_Reader
