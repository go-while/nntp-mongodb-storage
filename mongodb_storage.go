package mongostorage

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"context"
	"encoding/gob"
	"fmt"
	"github.com/go-while/go-utils"
	//"github.com/go-while/nntp-storage"
	"go.mongodb.org/mongo-driver/bson"
	//"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"io/ioutil"
	"log"
	"math/rand"
	"time"
	"sync"
)

// CONSTANTS comments written by AI.

// DefaultMongoURI is the default MongoDB connection string used when no URI is provided.
const DefaultMongoURI string = "mongodb://localhost:27017"

// DefaultMongoDatabaseName is the default name of the MongoDB database used when no database name is provided.
const DefaultMongoDatabaseName string = "nntp"

// DefaultMongoCollection is the default name of the MongoDB collection used when no collection name is provided.
const DefaultMongoCollection string = "articles"

// DefaultMongoTimeout is the default timeout value (in seconds) for connecting to MongoDB.
const DefaultMongoTimeout int64 = 15

// DefaultDelQueue sets the number of objects a DeleteWorker will queue before processing.
const DefaultDelQueue int = 2

// DefaultDelWorker sets the number of DeleteWorker instances to start by default.
const DefaultDelWorker int = 1

// DefaultDeleteBatchSize sets the number of Msgidhashes a DeleteWorker will cache before deleting to batch into one process.
const DefaultDeleteBatchSize int = 1

// DefaultInsQueue sets the number of objects an InsertWorker will queue before processing.
const DefaultInsQueue int = 2

// DefaultInsWorker sets the number of InsertWorker instances to start by default.
const DefaultInsWorker int = 1

// DefaultInsertBatchSize sets the number of Articles an InsertWorker will cache before inserting to batch into one process.
const DefaultInsertBatchSize int = 1

// DefaultGetQueue sets the total queue length for all ReaderWorker instances.
const DefaultGetQueue int = 2

// DefaultGetWorker sets the number of ReaderWorker instances to start by default.
const DefaultGetWorker int = 1

// Compression constants

// NOCOMP represents the value indicating no compression for articles.
const NOCOMP   int = 0

// GZIP_enc represents the value indicating GZIP compression for articles.
const GZIP_enc int = 1

// ZLIB_enc represents the value indicating ZLIB compression for articles.
const ZLIB_enc int = 2



// Note: The default values provided above are recommended for most use cases.
// However, these values can be adjusted according to your specific requirements.
// Be cautious when modifying these settings, as improper adjustments might lead to suboptimal performance or resource utilization.

var (
	Counter COUNTER
	// _queue channels handle requests for read/get, delete and insert
	Mongo_Reader_queue chan MongoReadRequest
	Mongo_Delete_queue chan string
	Mongo_Insert_queue chan MongoArticle
	// pass a bool [true|false] to the UpDN_ channels and workers will start or stop
	// external access via: mongostorage.UpDn_***_Worker_chan
	UpDn_StopAll_Worker_chan = make(chan bool, 1)
	UpDn_Reader_Worker_chan  = make(chan bool, 1)
	UpDn_Delete_Worker_chan  = make(chan bool, 1)
	UpDn_Insert_Worker_chan  = make(chan bool, 1)
	// internal channels to notify workers to stop
	stop_reader_worker_chan = make(chan int, 1)
	stop_delete_worker_chan = make(chan int, 1)
	stop_insert_worker_chan = make(chan int, 1)
	worker_status_chan = make(chan workerstatus, 65535)
	READER string = "reader"
	DELETE string = "delete"
	INSERT string = "insert"
) // end var

type workerstatus struct {
	wType string
	status update
}

type update struct {
	Did    int
	Bad    int
	Boot   bool
	Stop   bool
}

// MongoStorageConfig represents the parameters for configuring MongoDB and worker goroutines.
// You can adjust the number of worker goroutines and queue sizes based on your application's requirements and available resources.
type MongoStorageConfig struct {
	// MongoURI is a string representing the MongoDB connection URI.
	// It should include the necessary authentication details and the address of the MongoDB server.
	MongoURI string

	// MongoDatabaseName is a string representing the name of the MongoDB database where the articles will be stored and retrieved from.
	MongoDatabaseName string

	// MongoCollection is a string representing the name of the collection within the database where the articles will be stored.
	MongoCollection string

	// MongoTimeout is an int64 value defining the timeout for a connection attempt to the MongoDB server.
	// If the connection is not successful within this duration, it will time out.
	MongoTimeout int64

	// DelWorker is an integer specifying the number of worker goroutines to handle article deletions.
	// It determines the level of concurrency for deletion operations.
	// More DelWorker values can improve the speed at which articles are deleted from the database.
	DelWorker int

	// DelQueue is an integer specifying the size of the delete queue.
	// It specifies how many delete requests can be buffered before the send operation blocks.
	// If DelQueue is 0 or negative, a default value will be used.
	// The DelQueue parameter sets the maximum number of article deletion requests that can be buffered before the worker goroutines start processing them.
	DelQueue int

	// InsWorker is an integer specifying the number of worker goroutines to handle article insertions.
	// It determines the level of concurrency for insertion operations.
	// This parameter controls the concurrency for insertion operations, allowing multiple articles to be inserted simultaneously.
	InsWorker int

	// InsQueue is an integer specifying the size of the insert queue.
	// It specifies how many article insertion requests can be buffered before the send operation blocks.
	// If InsQueue is 0 or negative, a default value will be used.
	// The InsQueue parameter specifies the maximum number of article insertion requests that can be buffered in the queue before the worker goroutines process them.
	// If the number of pending insertions exceeds this limit, the send operation on the Mongo_Insert_queue channel will block.
	InsQueue int

	// GetWorker is an integer specifying the number of worker goroutines to handle article reads.
	// It determines the level of concurrency for read operations.
	// This parameter controls the level of concurrency for read operations, enabling multiple read requests to be processed concurrently.
	GetWorker int

	// GetQueue is an integer specifying the size of the read queue.
	// It specifies how many read requests can be buffered before the send operation blocks.
	// If GetQueue is 0 or negative, a default value will be used.
	// The GetQueue parameter defines the maximum length of the read request queue.
	// When the number of read requests exceeds this limit, the send operation on the Mongo_Reader_queue channel will block until space becomes available.
	GetQueue int

	// TestAfterInsert is a boolean flag indicating whether to perform a test after an article insertion.
	// The specific test details are not provided in the function, and the flag can be used for application-specific testing purposes.
	TestAfterInsert bool

	// DelBatch sets the number of Msgidhashes a DeleteWorker will cache before deleting to batch into one process.
	DelBatch int

	// InsBatch sets the number of Articles an InsertWorker will cache before inserting to batch into one process.
	InsBatch int
} // end type MongoStorageConfig

// MongoArticle represents an article stored in MongoDB.
// It contains the following fields:
// - MessageIDHash: The unique identifier (hash) of the article (mapped to the "_id" field in MongoDB).
// - MessageID: The Message-ID of the article (mapped to the "msgid" field in MongoDB).
// - Newsgroups: A slice of strings containing the newsgroups associated with the article (mapped to the "newsgroups" field in MongoDB).
// - Head: A byte slice representing the head (header) of the article (mapped to the "head" field in MongoDB).
// - Headsize: An integer representing the size of the head in bytes (mapped to the "hs" field in MongoDB).
// - Body: A byte slice representing the body (content) of the article (mapped to the "body" field in MongoDB).
// - Bodysize: An integer representing the size of the body in bytes (mapped to the "bs" field in MongoDB).
// - Enc: An integer representing the encoding type of the article (mapped to the "enc" field in MongoDB).
// - Found: A boolean indicating whether the article was found during retrieval (not mapped to MongoDB).
type MongoArticle struct {
	MessageIDHash *string `bson:"_id"`
	MessageID     *string `bson:"msgid"`
	Newsgroups    []string `bson:"newsgroups"`
	Head     []byte `bson:"head"`
	Headsize int    `bson:"hs"`
	Body     []byte `bson:"body"`
	Bodysize int    `bson:"bs"`
	Enc      int    `bson:"enc"`
	Found    bool
} // end type MongoArticle struct

// MongoReadReqReturn represents the return value for a read request in MongoDB.
// It contains the following field:
// - Articles: A slice of pointers to MongoArticle objects representing the fetched articles.
type MongoReadReqReturn struct {
	Articles []*MongoArticle
} // end type MongoReadReqReturn struct

// MongoReadRequest represents a read request for fetching articles from MongoDB.
// It contains the following fields:
//   - Msgidhashes: A slice of messageIDHashes for which articles are requested.
//   - RetChan: A channel to receive the fetched articles as []*MongoArticle.
//     The fetched articles will be sent through this channel upon successful retrieval.
type MongoReadRequest struct {
	Msgidhashes []*string
	RetChan     chan []*MongoArticle
} // end type MongoReadRequest struct

// MongoDeleteRequest represents a delete request for deleting articles from MongoDB.
// It contains the following fields:
// - Msgidhashes: A slice of messageIDHashes for which articles are requested to be deleted.
// - RetChan: A channel to receive the deleted articles as []*MongoArticle.
// The deleted articles will be sent through this channel upon successful deletion.
type MongoDeleteRequest struct {
	Msgidhashes []string
	RetChan     chan []*MongoArticle
} // end type MongoDeleteRequest struct

// GetDefaultMongoStorageConfig returns a MongoStorageConfig with default values.
func GetDefaultMongoStorageConfig() MongoStorageConfig {
	cfg := MongoStorageConfig{

		// Default MongoDB connection URI
		MongoURI: DefaultMongoURI,

		// Default MongoDB database name
		MongoDatabaseName: DefaultMongoDatabaseName,

		// Default MongoDB collection name
		MongoCollection: DefaultMongoCollection,

		// Default MongoDB timeout (in seconds) for connecting to the server
		MongoTimeout: DefaultMongoTimeout,

		// Default size of the delete queue (number of delete requests buffered before blocking)
		DelQueue: DefaultDelQueue,

		// Default number of delete worker goroutines
		DelWorker: DefaultDelWorker,

		// Default number of Msgidhashes a DeleteWorker will cache before deleting to batch into one process
		DelBatch: DefaultDeleteBatchSize,

		// Default size of the insert queue (number of insertion requests buffered before blocking)
		InsQueue: DefaultInsQueue,

		// Default number of insert worker goroutines
		InsWorker: DefaultInsWorker,

		// Default number of Articles an InsertWorker will cache before inserting to batch into one process
		InsBatch: DefaultInsertBatchSize,

		// Default size of the read queue (number of read requests buffered before blocking)
		GetQueue: DefaultGetQueue,

		// Default number of reader worker goroutines
		GetWorker: DefaultGetWorker,

		// Default test flag for performing a test after an article insertion
		TestAfterInsert: false, // You can change this to true if you want to perform a test after an article insertion.
	}

	return cfg
} // end func GetDefaultMongoStorageConfig

// SetDefaultsIfZero takes a pointer to a MongoStorageConfig and sets integer-typed fields to their default values
// if their current value is less than or equal to zero. It also sets default values for empty strings.
// This function is useful for ensuring that all the fields in the MongoStorageConfig have valid values.
func SetDefaultsIfZero(cfg *MongoStorageConfig) {
	if cfg == nil {
		return
	}

	if cfg.MongoURI == "" {
		cfg.MongoURI = DefaultMongoURI
	}
	if cfg.MongoDatabaseName == "" {
		cfg.MongoDatabaseName = DefaultMongoDatabaseName
	}
	if cfg.MongoCollection == "" {
		cfg.MongoCollection = DefaultMongoCollection
	}
	if cfg.MongoTimeout <= 0 {
		cfg.MongoTimeout = DefaultMongoTimeout
	}
	if cfg.DelQueue <= 0 {
		cfg.DelQueue = DefaultDelQueue
	}
	if cfg.DelWorker <= 0 {
		cfg.DelWorker = DefaultDelWorker
	}
	if cfg.DelBatch <= 0 {
		cfg.DelBatch = DefaultDeleteBatchSize
	}
	if cfg.InsQueue <= 0 {
		cfg.InsQueue = DefaultInsQueue
	}
	if cfg.InsWorker <= 0 {
		cfg.InsWorker = DefaultInsWorker
	}
	if cfg.InsBatch <= 0 {
		cfg.InsBatch = DefaultInsertBatchSize
	}
	if cfg.GetQueue <= 0 {
		cfg.GetQueue = DefaultGetQueue
	}
	if cfg.GetWorker <= 0 {
		cfg.GetWorker = DefaultGetWorker
	}
} // end func SetDefaultsIfZero

// Load_MongoDB initializes the MongoDB storage backend with the specified configuration parameters.
// The function starts the worker goroutines in separate background routines.
// function written by AI.
func Load_MongoDB(cfg *MongoStorageConfig) {
	//func Load_MongoDB(MongoURI string, MongoDatabaseName string, MongoCollection string, MongoTimeout int64, DelWorker int, DelQueue int, DelBatch int, InsWorker int, InsQueue int, InsBatch int, GetQueue int, GetWorker int, TestAfterInsert bool) {
	// Load_MongoDB initializes the mongodb storage backend
	SetDefaultsIfZero(cfg)

	Counter.Init() // = COUNTER{m: make(map[string]uint64) }

	Mongo_Delete_queue = make(chan string, cfg.DelQueue)
	Mongo_Insert_queue = make(chan MongoArticle, cfg.InsQueue)
	Mongo_Reader_queue = make(chan MongoReadRequest, cfg.GetQueue)
	log.Printf("Load_MongoDB: Reader GetQueue=%d GetWorker=%d", cfg.GetQueue, cfg.GetWorker)
	log.Printf("Load_MongoDB: Delete DelQueue=%d DelWorker=%d DelBatch=%d", cfg.DelQueue, cfg.DelWorker, cfg.DelBatch)
	log.Printf("Load_MongoDB: Insert InsQueue=%d InsWorker=%d InsBatch=%d", cfg.InsQueue, cfg.InsWorker, cfg.InsBatch)

	go workerStatus()
	go mongoWorker_UpDn_Scaler(cfg)
	for i := 1; i <= cfg.GetWorker; i++ {
		go MongoWorker_Reader(i, &READER, cfg)
	}
	for i := 1; i <= cfg.DelWorker; i++ {
		go MongoWorker_Delete(i, &DELETE, cfg)
	}
	for i := 1; i <= cfg.InsWorker; i++ {
		go MongoWorker_Insert(i, &INSERT, cfg)
	}

} // end func Load_MongoDB

// ConnectMongoDB is a function responsible for establishing a connection to the MongoDB server and accessing a specific collection.
// It takes the following parameters:
// - who: A string representing the name or identifier of the calling function or worker.
// function written by AI.
func ConnectMongoDB(who string, cfg *MongoStorageConfig) (context.Context, context.CancelFunc, *mongo.Client, *mongo.Collection, error) {

	client, err := mongo.NewClient(options.Client().ApplyURI(cfg.MongoURI))
	if err != nil {
		log.Printf("Error creating MongoDB client: %v", err)
		return nil, nil, nil, nil, err
	}

	// Set a timeout for the connection.
	newTimeout := time.Second * time.Duration(cfg.MongoTimeout)
	deadline := time.Now().Add(newTimeout)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	err = client.Connect(ctx)
	if err != nil {
		log.Printf("Error connecting to MongoDB: %v", err)
		cancel()
		return nil, nil, nil, nil, err
	}

	// Access the MongoDB collection.
	collection := client.Database(cfg.MongoDatabaseName).Collection(cfg.MongoCollection)

	log.Printf("-> ConnectMongoDB who=%s", who)
	return ctx, cancel, client, collection, err
} // end func ConnectMongoDB

// DisConnectMongoDB is a function responsible for disconnecting from the MongoDB server.
// function written by AI.
func DisConnectMongoDB(who string, ctx context.Context, client *mongo.Client) error {
	err := client.Disconnect(ctx)
	if err != nil {
		log.Printf("MongoDB Error disconnecting from MongoDB: %v", err)
		return err
	}
	log.Printf("<- DisConnectMongoDB who=%s", who)
	return nil
} // end func DisConnectMongoDB

// requeue_Articles requeues a slice of articles into the MongoDB insert queue.
func requeue_Articles(articles []*MongoArticle){
	log.Printf("Warn requeue_Articles: articles=%d", len(articles))

	for _, article := range articles {
		Mongo_Insert_queue <- *article
	}
} // end func requeue_Articles

// MongoWorker_Insert is a goroutine function responsible for processing incoming insert requests from the 'Mongo_Insert_queue'.
// It inserts articles into a MongoDB collection based on the articles provided in the 'Mongo_Insert_queue'.
// The function continuously listens for insert requests and processes them until the 'Mongo_Insert_queue' is closed.
// It accumulates the articles to be inserted until the number of articles reaches the limit set by cfg.InsBatch.
// Once the 'Mongo_Insert_queue' is closed, the function performs disconnection from the MongoDB server and terminates.
//
// If the 'TestAfterInsert' flag is set to true, the function will perform article existence checks after each insertion.
// The check logs the results of the existence test for each article.
// function partly written by AI.
func MongoWorker_Insert(wid int, wType *string, cfg *MongoStorageConfig) {
	if wid <= 0 {
		log.Printf("Error MongoWorker_Insert wid <= 0")
		return
	}
	did, bad := 0, 0
	updateWorkerStatus(wType, update{ Boot: true })
	defer updateWorkerStatus(wType, update{ Stop: true })
	reboot := false
	who := fmt.Sprintf("MongoWorker_Insert#%d", wid)
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
			log.Printf("Error MongoWorker_Insert ConnectMongoDB err='%v'", err)
			time.Sleep(time.Second * calculateExponentialBackoff(attempts))
			continue
		}
		break
	}
	timeOutTime := int64(1)
	timeout := time.After(time.Second * time.Duration(timeOutTime))
	articles := []*MongoArticle{}
	is_timeout := false
	var diff int64
	var last_insert int64
forever:
	for {
		do_insert := false
		len_arts := len(articles)
		if len_arts == cfg.InsBatch || is_timeout {
			diff = utils.UnixTimeSec() - last_insert

			if len_arts >= cfg.InsBatch {
				do_insert = true
			} else if is_timeout && len_arts > 0 && diff > timeOutTime {
				do_insert = true
			}
			if is_timeout {
				is_timeout = false
			}

			if do_insert {
				log.Printf("Pre-Ins Many msgidhashes=%d", len_arts)
				did += len_arts
				ctx, cancel = extendContextTimeout(ctx, cancel, cfg.MongoTimeout)
				if err := MongoInsertManyArticles(ctx, collection, articles); err != nil {
					// connection error
					go requeue_Articles(articles)
					articles = []*MongoArticle{}
					reboot = true
				}
				if cfg.TestAfterInsert {
					for _, article := range articles {
						ctx, cancel = extendContextTimeout(ctx, cancel, cfg.MongoTimeout)
						if retbool, err := CheckIfArticleExistsByMessageIDHash(ctx, collection, article.MessageIDHash); retbool {
							// The article with the given hash exists.
							log.Printf("article exists: %s", *article.MessageIDHash)
						} else if err != nil {
							log.Printf("Error CheckIfArticleExistsByMessageIDHash: %s err %v", *article.MessageIDHash, err)
						}
					}
				}
				last_insert = utils.UnixTimeSec()
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
			//log.Printf("%s process Mongo_Insert_queue", who)
			articles = append(articles, &article)
			if len(articles) >= cfg.InsBatch {
				break insert_queue
			}
		case <-timeout:
			is_timeout = true
			if did > 0 || bad > 0 {
				updateWorkerStatus(wType, update{ Did: did, Bad: bad })
				did, bad = 0, 0
			}
			maxID := iStop_Worker("insert")
			if wid > maxID {
				log.Printf("-- Stopping %s", who)
				break forever // stop Worker
			}
			timeout = time.After(time.Second * time.Duration(timeOutTime))
			//log.Printf("MongoWorker_Insert alive hashs=%d", len(msgidhashes))
		} // end select insert_queue
	} // end for forever
	if len(articles) > 0 {
		if err := MongoInsertManyArticles(ctx, collection, articles); err != nil {
			// connection error
			go requeue_Articles(articles)
			articles = []*MongoArticle{}
			reboot = true
		}
		did += len(articles)
	}
	DisConnectMongoDB(who, ctx, client)
	updateWorkerStatus(wType, update{ Did: did, Bad: bad })
	log.Printf("xx End %s reboot=%t", who, reboot)
	if reboot {
		go MongoWorker_Insert(wid, wType, cfg)
	}
} // end func MongoWorker_Insert

// MongoWorker_Delete is a goroutine function responsible for processing incoming delete requests from the 'Mongo_Delete_queue'.
// It deletes articles from a MongoDB collection based on the given MessageIDHashes provided in the 'Mongo_Delete_queue'.
// The function continuously listens for delete requests and processes them until the 'Mongo_Delete_queue' is closed.
// Note: If cfg.DelBatch is set to 1, the function will delete articles one by one, processing individual delete requests from the queue.
// Otherwise, it will delete articles in bulk based on the accumulated MessageIDHashes.
// function partly written by AI.
func MongoWorker_Delete(wid int, wType *string, cfg *MongoStorageConfig) {
	if wid <= 0 {
		log.Printf("Error MongoWorker_Delete wid <= 0")
		return
	}
	did, bad := 0, 0
	updateWorkerStatus(wType, update{ Boot: true })
	defer updateWorkerStatus(wType, update{ Stop: true })
	who := fmt.Sprintf("MongoWorker_Delete#%d", wid)
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
			log.Printf("Error MongoWorker_Insert ConnectMongoDB err='%v'", err)
			time.Sleep(time.Second * calculateExponentialBackoff(attempts))
			continue
		}
		break
	}
	timeOutTime := int64(1)
	timeout := time.After(time.Second * time.Duration(timeOutTime))
	msgidhashes := []string{}
	is_timeout := false
	var diff int64
	var last_delete int64
forever:
	for {
		len_hashs := len(msgidhashes)
		do_delete := false
		if len_hashs == cfg.DelBatch || is_timeout {
			diff = utils.UnixTimeSec() - last_delete
			if len_hashs >= cfg.DelBatch {
				do_delete = true
			} else if is_timeout && len_hashs > 0 && diff > timeOutTime {
				do_delete = true
			}
			if is_timeout {
				is_timeout = false
			}
		} // check if do_delete

		if do_delete {
			log.Printf("Pre-Del Many msgidhashes=%d", len_hashs)
			ctx, cancel = extendContextTimeout(ctx, cancel, cfg.MongoTimeout)
			MongoDeleteManyArticles(ctx, collection, msgidhashes) // TODO catch error !
			msgidhashes = []string{}
			last_delete = utils.UnixTimeSec()
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
			//log.Printf("%s process Mongo_Delete_queue", who)
			if cfg.DelBatch == 1 { // deletes articles one by one
				for messageIDHash := range Mongo_Delete_queue {
					log.Printf("Pre-Del One msgidhash='%s'", msgidhash)
					ctx, cancel = extendContextTimeout(ctx, cancel, cfg.MongoTimeout)
					err := deleteArticlesByMessageIDHash(ctx, collection, messageIDHash)
					if err != nil {
						log.Printf("Error deleting messageIDHash=%s err='%v'", messageIDHash, err)
						bad++
						continue
					}
					log.Printf("Deleted messageIDHash=%s", messageIDHash)
				}
			} else {
				msgidhashes = append(msgidhashes, msgidhash)
				//log.Printf("Append Del Worker: msgidhash='%s' to msgidhashes=%d", msgidhash, len(msgidhashes))
			}
			if len(msgidhashes) >= cfg.DelBatch {
				break select_delete_queue
			}
		case <-timeout:
			is_timeout = true
			if did > 0 || bad > 0 {
				updateWorkerStatus(wType, update{ Did: did, Bad: bad })
				did, bad = 0, 0
			}
			maxID := iStop_Worker("delete")
			if wid > maxID {
				log.Printf("-- Stopping %s", who)
				break forever // stop Worker
			}
			timeout = time.After(time.Second * time.Duration(timeOutTime))
			//log.Printf("MongoWorker_Delete alive hashs=%d", len(msgidhashes))
			//break select_delete_queue
		} // end select delete_queue
	} // end for forever
	if len(msgidhashes) > 0 {
		MongoDeleteManyArticles(ctx, collection, msgidhashes) // TODO: catch error !
	}
	DisConnectMongoDB(who, ctx, client)
	updateWorkerStatus(wType, update{ Did: did, Bad: bad })
	log.Printf("xx End %s", who)
} // end func MongoWorker_Delete

// MongoWorker_Reader is a goroutine function responsible for processing incoming read requests from the 'Mongo_Reader_queue'.
// It reads articles from a MongoDB collection based on the given MessageIDHashes provided in the 'readreq' parameter.
// The function continuously listens for read requests and processes them until the 'Mongo_Reader_queue' is closed.
// Once the 'Mongo_Reader_queue' is closed, the function performs disconnection from the MongoDB server and terminates.
// function partly written by AI.
func MongoWorker_Reader(wid int, wType *string, cfg *MongoStorageConfig) {
	if wid <= 0 {
		log.Printf("Error MongoWorker_Reader wid <= 0")
		return
	}
	did, bad := 0, 0
	updateWorkerStatus(wType, update{ Boot: true })
	defer updateWorkerStatus(wType, update{ Stop: true })
	who := fmt.Sprintf("MongoWorker_Reader#%d", wid)
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
			log.Printf("Error MongoWorker_Reader ConnectMongoDB err='%v'", err)
			time.Sleep(time.Second * calculateExponentialBackoff(attempts))
			continue
		}
		break
	}
	timeOutTime := time.Duration(time.Second * 1)
	timeout := time.After(time.Second * time.Duration(timeOutTime))
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
			//log.Printf("%s process Mongo_Reader_queue", who)
			did++
			ctx, cancel = extendContextTimeout(ctx, cancel, cfg.MongoTimeout)
			// Read articles for the given msgidhashes.
			articles, err := readArticlesByMessageIDHashes(ctx, collection, readreq.Msgidhashes)
			if err != nil {
				bad++
				log.Printf("Error readArticlesByMessageIDHashes hashs=%d err='%v'", len(readreq.Msgidhashes), err)
				reboot = true
				break forever
			}
			len_request := len(readreq.Msgidhashes)
			len_got_arts := len(articles)
			if readreq.RetChan != nil {
				log.Printf("passing response %d/%d read articles to readreq.RetChan", len_got_arts, len_request)
				readreq.RetChan <- articles // MongoReadReqReturn{ Articles: articles }
				// sender does not close the readreq.RetChan here so it can be reused for next read request
			} else {
				log.Printf("WARN got %d/%d read articles readreq.RetChan=nil", len_got_arts, len_request)
			}
			// Do something with the articles, e.g., handle them or send them to another channel.
		case <-timeout:
			//is_timeout = true
			if did > 0 || bad > 0 {
				updateWorkerStatus(wType, update{ Did: did, Bad: bad })
				did, bad = 0, 0
			}
			maxID := iStop_Worker("reader")
			if wid > maxID {
				log.Printf("-- Stopping %s", who)
				break forever
			}
			time.After(time.Second * time.Duration(timeOutTime))
			//log.Printf("MongoWorker_Reader alive hashs=%d", len(msgidhashes))
			//break reader_queue
		} // end select
	}
	DisConnectMongoDB(who, ctx, client)
	updateWorkerStatus(wType, update{ Did: did, Bad: bad })
	log.Printf("xx End %s", who)
	if reboot {
		go MongoWorker_Reader(wid, wType, cfg)
	}
} // end func MongoWorker_Reader

// MongoInsertOneArticle is a function that inserts a single article into a MongoDB collection.
// function written by AI.
func MongoInsertOneArticle(ctx context.Context, collection *mongo.Collection, article *MongoArticle) error {
	_, err := collection.InsertOne(ctx, article)
	if err != nil {
		log.Printf("Error collection.InsertOne err='%v'", err)
	}
	return err
} // end func MongoInsertOneArticle

// MongoInsertManyArticles is a function that performs a bulk insert of multiple articles into a MongoDB collection.
// function written by AI.
func MongoInsertManyArticles(ctx context.Context, collection *mongo.Collection, articles []*MongoArticle) error {
	insert_articles := []interface{}{}
	for _, article := range articles {
		insert_articles = append(insert_articles, article)
	}
	/*
			* Unordered Insert:
		   *   If you set the ordered option to false, MongoDB will continue the insertMany operation even if a duplicate key is found.
		   *   The operation will try to insert all the documents in the array, and duplicates will be ignored.
		   *   The first occurrence of each unique _id will be inserted, and subsequent occurrences will be skipped.
	*/
	opts := options.InsertMany().SetOrdered(false)
	result, err := collection.InsertMany(ctx, insert_articles, opts)
	if err != nil {
		//log.Printf("Warn MongoInsertManyArticles err='%v' inserted=%d", err, len(result.InsertedIDs))
		if writeErrors, ok := err.(mongo.WriteErrors); ok {
			// Handle individual write errors for each document.
			for _, writeError := range writeErrors {
				if writeError.Code == 11000 { // Duplicate key error code
					// Handle duplicate key error here.
					//log.Printf("Duplicate key error for document: %v", writeError.ID)
					continue
				} else {
					// Handle other write errors, if needed.
					log.Printf("Error MongoInsertManyArticles Other insert error code=%d", writeError.Code)
					continue
				}
			}
		} else {
			// Handle general connection or other error.
			log.Printf("Error MongoInsertManyArticles err='%v'", err)
			return err
		}
		return err
	} else // end result InsertMany err != nil
	if len(result.InsertedIDs) == len(articles) {
		log.Printf("MongoInsertManyArticles: inserted=%d/%d", len(result.InsertedIDs), len(articles))
	}
	return nil
} // end func MongoInsertManyArticles

// MongoDeleteManyArticles is responsible for deleting multiple articles from the MongoDB collection based on a given set of MessageIDHashes.
// function written by AI.
func MongoDeleteManyArticles(ctx context.Context, collection *mongo.Collection, msgidhashes []string) bool {
	// Build the filter for DeleteMany
	filter := bson.M{
		"_id": bson.M{
			"$in": msgidhashes,
		},
	}

	// Perform the DeleteMany operation
	result, err := collection.DeleteMany(ctx, filter)
	if err != nil {
		log.Printf("Error MongoDeleteManyArticles err='%v'", err)
		return false
	}

	log.Printf("MongoDB Deleted many=%d", result.DeletedCount)
	return result.DeletedCount == int64(len(msgidhashes))
} // end func MongoDeleteManyArticles

// extendContextTimeout extends the deadline of a given context by canceling the previous
// function written by AI.
func extendContextTimeout(ctx context.Context, cancel context.CancelFunc, MongoTimeout int64) (context.Context, context.CancelFunc) {
	//log.Printf("extendContextTimeout")
	cancel()
	newTimeout := time.Second * time.Duration(MongoTimeout)
	deadline := time.Now().Add(newTimeout)
	ctx, cancel = context.WithDeadline(context.Background(), deadline)
	return ctx, cancel
} // end func extendContextTimeout

// deleteArticlesByMessageIDHash deletes an article from the MongoDB collection by its MessageIDHash.
// function written by AI.
func deleteArticlesByMessageIDHash(ctx context.Context, collection *mongo.Collection, messageIDHash string) error {
	// Filter to find the articles with the given MessageIDHash.
	filter := bson.M{"_id": messageIDHash}

	// Delete the articles with the given MessageIDHash.
	_, err := collection.DeleteMany(ctx, filter)
	if err != nil {
		return err
	}
	return nil
} // end func deleteArticlesByMessageIDHash

// retrieveArticleByMessageIDHash retrieves an article from the MongoDB collection by its MessageIDHash.
// function written by AI.
func retrieveArticleByMessageIDHash(ctx context.Context, collection *mongo.Collection, messageIDHash string) (*MongoArticle, error) {
	// Filter to find the article with the given MessageIDHash.
	filter := bson.M{"_id": messageIDHash}

	// Find the article in the collection.
	result := collection.FindOne(ctx, filter)

	// Check if the article exists.
	if result.Err() != nil {
		// Check if the error is due to "no documents in result".
		if result.Err() == mongo.ErrNoDocuments {
			log.Printf("Info retrieveArticleByMessageIDHash not found hash=%s", messageIDHash)
			return nil, nil
		}
		// Return other errors as they indicate a problem with the query.
		return nil, result.Err()
	}

	// Decode the article from the BSON representation to a MongoArticle object.
	var article MongoArticle
	if err := result.Decode(&article); err != nil {
		log.Printf("Error retrieveArticleByMessageIDHash result.Decode err='%v'", err)
		return nil, err
	}

	return &article, nil
} // end func retrieveArticleByMessageIDHash

// readArticlesByMessageIDHashes is a function that retrieves articles from the MongoDB collection based on a list of MessageIDHashes.
// function written by AI.
func readArticlesByMessageIDHashes(ctx context.Context, collection *mongo.Collection, msgidhashes []*string) ([]*MongoArticle, error) {
	// Filter to find the articles with the given MessageIDHashes.
	filter := bson.M{"_id": bson.M{"$in": msgidhashes}}

	// Find the articles in the collection.
	cursor, err := collection.Find(ctx, filter)
	if err != nil {
		log.Printf("Error readArticlesByMessageIDHashes coll.Find err='%v'", err)
		return nil, err
	}
	defer cursor.Close(ctx)

	// Decode the articles from the BSON representation to MongoArticle objects.
	var articles []*MongoArticle
	var founds []*string
	for cursor.Next(ctx) {
		var article MongoArticle
		if err := cursor.Decode(&article); err != nil {
			log.Printf("Error readArticlesByMessageIDHashes cursor.Decode article err='%v'", err)
			return nil, err
		}
		article.Found = true
		articles = append(articles, &article)
		founds = append(founds, article.MessageIDHash)
	}
	for _, hash := range msgidhashes {
		if !isPStringInSlice(founds, hash) {
			log.Printf("readArticlesByMessageIDHashes notfound hash='%s'", *hash)
			var article MongoArticle
			article.MessageIDHash = hash
			articles = append(articles, &article)
		}
	}
	if err := cursor.Err(); err != nil {
		log.Printf("Error readArticlesByMessageIDHashes cursor.Err='%v'", err)
		return nil, err
	}

	return articles, nil
} // end func readArticlesByMessageIDHashes

// sliceContains checks if a given target string exists in the provided slice of strings.
// function written by AI.
func sliceContains(slice []string, target string) bool {
	for _, item := range slice {
		if item == target {
			return true
		}
	}
	return false
} // end func sliceContains

// isPStringInSlice checks if a target string pointer exists in a slice of string pointers.
// function written by AI.
func isPStringInSlice(slice []*string, target *string) bool {
	for _, s := range slice {
		if s != nil && target != nil && *s == *target {
			return true
		}
	}
	return false
} // end func isPStringInSlice

// RetrieveHeadByMessageIDHash is a function that retrieves the "Head" data of an article based on its MessageIDHash.
// function written by AI.
func RetrieveHeadByMessageIDHash(ctx context.Context, collection *mongo.Collection, messageIDHash string) ([]byte, error) {
	// Filter to find the article with the given "messageIDHash".
	filter := bson.M{"_id": messageIDHash}

	// Projection to select only the "Head" field.
	projection := bson.M{"head": 1}

	// Find the article in the collection and select only the "Head" field.
	result := collection.FindOne(ctx, filter, options.FindOne().SetProjection(projection))

	// Check if the article exists.
	if result.Err() != nil {
		// Check if the error is due to "no documents in result".
		if result.Err() == mongo.ErrNoDocuments {
			return nil, nil
		}
		// Return other errors as they indicate a problem with the query.
		return nil, result.Err()
	}

	// Decode the "Head" from the BSON representation to a byte slice.
	var article MongoArticle
	if err := result.Decode(&article); err != nil {
		return nil, err
	}

	return article.Head, nil
} // end func retrieveHeadByMessageIDHash

// RetrieveBodyByMessageIDHash is a function that retrieves the "Body" data of an article  based on its MessageIDHash.
// function written by AI.
func RetrieveBodyByMessageIDHash(ctx context.Context, collection *mongo.Collection, messageIDHash string) ([]byte, error) {
	// Filter to find the article with the given "messageIDHash".
	filter := bson.M{"_id": messageIDHash}

	// Projection to select only the "Body" field.
	projection := bson.M{"body": 1}

	// Find the article in the collection and select only the "Body" field.
	result := collection.FindOne(ctx, filter, options.FindOne().SetProjection(projection))

	// Check if the article exists.
	if result.Err() != nil {
		// Check if the error is due to "no documents in result".
		if result.Err() == mongo.ErrNoDocuments {
			return nil, nil
		}
		// Return other errors as they indicate a problem with the query.
		return nil, result.Err()
	}

	// Decode the "Body" from the BSON representation to a byte slice.
	var article MongoArticle
	if err := result.Decode(&article); err != nil {
		return nil, err
	}

	return article.Body, nil
} // end func retrieveBodyByMessageIDHash

// RetrieveBodyByMessageIDHash is a function that retrieves the "Body" data of an article based on its MessageIDHash.
// function written by AI.
func CheckIfArticleExistsByMessageIDHash(ctx context.Context, collection *mongo.Collection, messageIDHash *string) (bool, error) {
	// Filter to find the articles with the given MessageIDHash.
	filter := bson.M{"_id": messageIDHash}
	result := collection.FindOne(ctx, filter, nil)
	if result.Err() != nil {
		// Check if the error is due to "no documents in result".
		if result.Err() == mongo.ErrNoDocuments {
			return false, nil
		}
		// Return other errors as they indicate a problem with the query.
		return false, result.Err()
	}
	// The document with the given MessageIDHash exists in the collection.
	return true, nil
} // end func CheckIfArticleExistsByMessageIDHash

// function written by AI.
func EncodeToGob(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(data); err != nil {
		return nil, fmt.Errorf("Error encodeToGob err='%v'", err)
	}
	return buf.Bytes(), nil
} // end func EncodeToGob

// CompressData is a function that takes an input byte slice 'input' and an integer 'algo' representing the compression algorithm.
// It compresses the input data using the specified compression algorithm and returns the compressed data as a new byte slice.
// function written by AI.
func CompressData(input []byte, algo int) ([]byte, error) {
	switch algo {
	case GZIP_enc:
		var buf bytes.Buffer
		zWriter, err := gzip.NewWriterLevel(&buf, 1)
		if err != nil {
			log.Printf("Error CompressData gzip err='%v'", err)
			return nil, err
		}
		zWriter.Write(input)
		zWriter.Flush()
		zWriter.Close()
		return buf.Bytes(), nil
	case ZLIB_enc:
		var buf bytes.Buffer
		zWriter, err := zlib.NewWriterLevel(&buf, 1)
		if err != nil {
			log.Printf("Error CompressData zlib err='%v'", err)
			return nil, err
		}
		zWriter.Write(input)
		zWriter.Flush()
		zWriter.Close()
		return buf.Bytes(), nil
	default:
		return nil, fmt.Errorf("unsupported compression algorithm: %d", algo)
	}
} // end func CompressData

// DecompressData is a function that takes an input byte slice 'input' and an integer 'algo' representing the compression algorithm.
// It decompresses the input data using the specified compression algorithm and returns the decompressed data as a new byte slice.
// function written by AI.
func DecompressData(input []byte, algo int) ([]byte, error) {
	switch algo {
	case GZIP_enc:
		zReader, err := gzip.NewReader(bytes.NewReader(input))
		if err != nil {
			return nil, err
		}
		zReader.Close()
		return ioutil.ReadAll(zReader)
	case ZLIB_enc:
		zReader, err := zlib.NewReader(bytes.NewReader(input))
		if err != nil {
			return nil, err
		}
		zReader.Close()
		return ioutil.ReadAll(zReader)
	default:
		return nil, fmt.Errorf("unsupported compression algorithm: %d", algo)
	}
} // end func DecompressData

// calculateExponentialBackoff is a function that takes an integer 'attempt' representing the current attempt number for a retry operation.
// It calculates the backoff duration to be used before the next retry based on exponential backoff with jitter.
// function written by AI.
func calculateExponentialBackoff(attempt int) time.Duration {
	maxbackoff := time.Duration(30000)
	backoffBase := 100 * time.Millisecond // Base backoff duration (adjust as needed)
	backoffFactor := 2                    // Backoff factor (adjust as needed)

	// Calculate the backoff duration with exponential increase
	backoffDuration := time.Duration(backoffFactor<<uint(attempt-1)) * backoffBase
	if backoffDuration > maxbackoff {
		backoffDuration = maxbackoff
	}
	// Add some jitter to prevent all clients from retrying simultaneously
	jitter := time.Duration(rand.Int63n(int64(backoffBase / 2)))
	return backoffDuration + jitter
} // end func calculateExponentialBackoff

// MongoWorker_UpDN_Random periodically sends random up/down (true/false) signals to the worker channels.
// The purpose of this function is to simulate random up/down requests to control the worker
// function not written by AI.
// ./mongodbtest -randomUpDN -test-num 0
func MongoWorker_UpDN_Random() {
	isleep := 1
	log.Print("Start mongostorage.MongoWorker_UpDN_Random")
	for {
		arandA := rand.Intn(2)
		arandB := rand.Intn(4)
		time.Sleep(time.Second * time.Duration(isleep))
		sendbool := false
		switch arandA {
		case 1:
			sendbool = true
		default:
		}
		switch arandB {
		case 0:
			//wType = "reader"
			log.Printf("~~ UpDN_Random sending %t to UpDn_Reader_Worker_chan", sendbool)
			UpDn_Reader_Worker_chan <- sendbool
		case 1:
			//wType = "delete"
			log.Printf("~~ UpDN_Random sending %t to UpDn_Delete_Worker_chan", sendbool)
			UpDn_Delete_Worker_chan <- sendbool
		case 2:
			//wType = "insert"
			log.Printf("~~ UpDN_Random sending %t to UpDn_Insert_Worker_chan", sendbool)
			UpDn_Insert_Worker_chan <- sendbool
		case 3:
			//wType = "StopAll"
			log.Printf("~~ UpDN_Random sending %t to UpDn_StopAll_Worker_chan", sendbool)
			UpDn_StopAll_Worker_chan <- sendbool
		default:
		}
	}
} // end func MongoWorker_UpDN_Random

// The iStop_Worker function is responsible for stopping a specific type of worker (reader, delete, or insert)
// The function uses channels to communicate with the worker goroutines and control their termination.
// The function uses a switch statement to identify the type of worker to stop.
// For each type, it reads the current maximum worker ID from the corresponding channel.
// After retrieving the ID, it immediately writes it back to the channel, effectively "parking" the value again, so other parts of the code can still access the ID.
// function not written by AI.
func iStop_Worker(wType string) int {
	var maxwid int
	switch wType {
	case "reader":
		maxwid = <-stop_reader_worker_chan // read value
		stop_reader_worker_chan <- maxwid  // park value again
	case "delete":
		maxwid = <-stop_delete_worker_chan // read value
		stop_delete_worker_chan <- maxwid  // park value again
	case "insert":
		maxwid = <-stop_insert_worker_chan // read value
		stop_insert_worker_chan <- maxwid  // park value again
	default:
		log.Printf("Error iStop_Worker unknown Wtype=%s", wType)
	} // end switch wType
	return maxwid
} // fund end iStop_Worker

// updn_Set internally updates the worker count of the specified type and starts worker goroutines accordingly.
// function not written by AI.
func updn_Set(wType string, maxwid int, cfg *MongoStorageConfig) {
	var oldval int
	switch wType {
	case "reader":
		oldval = <-stop_reader_worker_chan // read old value
		stop_reader_worker_chan <- maxwid  // set new value
	case "delete":
		oldval = <-stop_delete_worker_chan // read old value
		stop_delete_worker_chan <- maxwid  // set new value
	case "insert":
		oldval = <-stop_insert_worker_chan // read old value
		stop_insert_worker_chan <- maxwid  // set new value
	default:
		log.Printf("Error updn_Set unknown Wtype=%s", wType)
	} // end switch wType

	if maxwid > oldval {
		// start new worker routines for wType
		switch wType {
		case "reader":
			for i := oldval + 1; i <= maxwid; i++ {
				go MongoWorker_Reader(i, &READER, cfg)
			}
		case "delete":
			for i := oldval + 1; i <= maxwid; i++ {
				go MongoWorker_Delete(i, &DELETE, cfg)
			}
		case "insert":
			for i := oldval + 1; i <= maxwid; i++ {
				go MongoWorker_Insert(i, &INSERT, cfg)
			}
		default:
			log.Printf("Error updn_Set unknown Wtype=%s", wType)
		} // end switch wType
	}
	log.Printf("$$ updn_Set wType=%s oldval=%d maxwid=%d", wType, oldval, maxwid)
} // end func updn_Set

// mongoWorker_UpDn_Scaler runs in the background and listens on channels for up/down requests to start/stop workers.
// Explanation:
// mongoWorker_UpDn_Scaler is responsible for managing the scaling of worker goroutines based on up/down requests.
// It listens to UpDn_*_Worker_chan channels to receive requests for starting or stopping specific types of workers.
// The function uses updn_Set to update the worker counts accordingly, which effectively starts or stops worker goroutines.
// This mechanism allows the application to dynamically adjust the number of worker goroutines based on the workload
// or other factors.
// Note: The function runs in the background and continues to process requests as they arrive.
//
// function not written by AI.
func mongoWorker_UpDn_Scaler(cfg *MongoStorageConfig) { // <-- needs load inital values
	// load initial values into channels
	stop_reader_worker_chan <- cfg.GetWorker
	stop_delete_worker_chan <- cfg.DelWorker
	stop_insert_worker_chan <- cfg.InsWorker
	atimeout := time.Duration(time.Second * 5)

	// The anonymous goroutine periodically checks the UpDn_StopAll_Worker_chan channel for messages.
	// If a "true" value is received, it sends "false" to all UpDn_*_Worker channels
	//   (UpDn_Reader_Worker_chan, UpDn_Delete_Worker_chan, and UpDn_Insert_Worker_chan)
	// to signal the worker goroutines to stop gracefully.
	// If a "false" value is received, no action is taken.
	// The goroutine also logs a message every N seconds to indicate that it is alive and running.
	// This functionality allows dynamically scaling the number of worker goroutines based on received up/down requests
	// while keeping track of their status and allowing controlled termination.
	go func(getWorker int, delWorker int, insWorker int) {
		timeout := time.After(atimeout)
		for {
			select {
			case <-timeout:
				timeout = time.After(atimeout)
				//log.Printf("UpDn_StopAll_Worker_chan alive")

			case retbool := <-UpDn_StopAll_Worker_chan:
				switch retbool {
				case true:
					// pass a false to all UpDn_*_Worker channels to stop them
					for i := 1; i <= getWorker; i++ {
						UpDn_Reader_Worker_chan <- false
					}
					for i := 1; i <= delWorker; i++ {
						UpDn_Delete_Worker_chan <- false
					}
					for i := 1; i <= insWorker; i++ {
						UpDn_Insert_Worker_chan <- false
					}
				case false:
					// pass here, will not do anything
					// to stop all workers
					//   pass a single true to UpDn_StopAll_Worker_chan
				} // end switch retbool
			} // end select
		} // end for
	}(cfg.GetWorker, cfg.DelWorker, cfg.InsWorker)
	time.Sleep(time.Second / 1000)

	// The anonymous goroutine continuously listens to the UpDn_Reader_Worker_chan channel for messages.
	// If a "true" value is received, it increments the GetWorker variable to increase the number of reader worker goroutines.
	// If a "false" value is received, it decrements GetWorker to decrease the number of reader worker goroutines.
	// After processing the message, the goroutine calls the updn_Set function to update the reader worker count with the new value (GetWorker).
	// The goroutine also logs a message every N seconds to indicate that it is alive and running.
	// The time.Sleep function is used to slightly delay the execution to avoid consuming excessive resources.
	// This functionality allows dynamically scaling the number of reader worker goroutines based on received up/down requests
	// and keeping track of their status while ensuring controlled termination and worker count adjustment.
	go func(getWorker int, wType string) {
		timeout := time.After(atimeout)
		for {
			select {
			case <-timeout:
				timeout = time.After(atimeout)
				//log.Printf("UpDn_Reader_Worker_chan alive")

			case retbool := <-UpDn_Reader_Worker_chan:
				switch retbool {
				case true:
					getWorker++
				case false:
					if getWorker > 0 {
						getWorker--
					}
				} // end switch retbool
				updn_Set(wType, getWorker, cfg)
			} // end select
		} // end for
	}(cfg.GetWorker, "reader")
	time.Sleep(time.Second / 1000)

	// The anonymous goroutine continuously listens to the UpDn_Delete_Worker_chan channel for messages.
	// If a "true" value is received, it increments the DelWorker variable to increase the number of delete worker goroutines.
	// If a "false" value is received, it decrements DelWorker to decrease the number of delete worker goroutines.
	// After processing the message, the goroutine calls the updn_Set function to update the delete worker count with the new value (DelWorker).
	// The goroutine also logs a message every N seconds to indicate that it is alive and running.
	// The time.Sleep function is used to slightly delay the execution to avoid consuming excessive resources.
	//This functionality allows dynamically scaling the number of delete worker goroutines based on received up/down requests
	// and keeping track of their status while ensuring controlled termination and worker count adjustment.
	go func(delWorker int, wType string) {
		timeout := time.After(atimeout)
		for {
			select {
			case <-timeout:
				timeout = time.After(atimeout)
				//log.Printf("UpDn_Delete_Worker_chan alive")

			case retbool := <-UpDn_Delete_Worker_chan:
				switch retbool {
				case true:
					delWorker++
				case false:
					if delWorker > 0 {
						delWorker--
					}
				} // end switch retbool
				updn_Set(wType, delWorker, cfg)
			} // end select
		} // end for
	}(cfg.DelWorker, "delete")
	time.Sleep(time.Second / 1000)

	// The anonymous goroutine continuously listens to the UpDn_Insert_Worker_chan channel for messages.
	// If a "true" value is received, it increments the InsWorker variable to increase the number of insert worker goroutines.
	// If a "false" value is received, it decrements InsWorker to decrease the number of insert worker goroutines.
	// After processing the message, the goroutine calls the updn_Set function to update the insert worker count with the new value (InsWorker).
	// The goroutine also logs a message every N seconds to indicate that it is alive and running.
	// The time.Sleep function is used to slightly delay the execution to avoid consuming excessive resources.
	// This functionality allows dynamically scaling the number of insert worker goroutines based on received up/down requests
	// and keeping track of their status while ensuring controlled termination and worker count adjustment.
	go func(insWorker int, wType string) {
		timeout := time.After(atimeout)
		for {
			select {
			case <-timeout:
				timeout = time.After(atimeout)
				//log.Printf("UpDn_Insert_Worker_chan alive")

			case retbool := <-UpDn_Insert_Worker_chan:
				switch retbool {
				case true:
					insWorker++
				case false:
					if insWorker > 0 {
						insWorker--
					}
				} // end switch retbool
				updn_Set(wType, insWorker, cfg)
			} // end select
		} // end for
	}(cfg.InsWorker, "insert")
	time.Sleep(time.Second / 1000)

} // end func mongoWorker_UpDn_Scaler

func updateWorkerStatus(wType *string, status update) {
	worker_status_chan <- workerstatus{ wType: *wType, status: status }
}

func workerStatus() {
	// worker status counter map
	// prevents workers from calling sync.Mutex
	// workers send updates into worker_status_chan
	counter := make(map[string]map[string]uint64)
	counter["reader"] = make(map[string]uint64)
	counter["delete"] = make(map[string]uint64)
	counter["insert"] = make(map[string]uint64)
	//timeout := time.After(time.Millisecond * 2500)
	for {
		select {
			/*
			case <- timeout:

				Counter.Set("Did_MongoWorker_Reader", counter["reader"]["did"])
				Counter.Set("Did_MongoWorker_Delete", counter["delete"]["did"])
				Counter.Set("Did_MongoWorker_Insert", counter["insert"]["did"])

				Counter.Set("Running_MongoWorker_Reader", counter["reader"]["run"])
				Counter.Set("Running_MongoWorker_Delete", counter["delete"]["run"])
				Counter.Set("Running_MongoWorker_Insert", counter["insert"]["run"])

				timeout = time.After(time.Millisecond * 2500)
			*/
			case nu := <- worker_status_chan:

				if nu.status.Did > 0 {
					counter[nu.wType]["did"] += uint64(nu.status.Did)
				}
				if nu.status.Bad > 0 {
					counter[nu.wType]["bad"] += uint64(nu.status.Bad)
				}
				if nu.status.Boot {
					counter[nu.wType]["run"]++
				} else
				if nu.status.Stop {
					counter[nu.wType]["run"]--
				}

		} // end select
	} // end for
} // end func WorkerStatus


type COUNTER struct {
	m map[string]uint64
	mux sync.Mutex
}

func (c *COUNTER) Init() {
	c.mux.Lock()
	c.m = make(map[string]uint64, 8)
	c.mux.Unlock()
} // end func Counter.Init

func (c *COUNTER) Inc(name string) {
	c.mux.Lock()
	c.m[name]++
	c.mux.Unlock()
} // end func Counter.Inc

func (c *COUNTER) Dec(name string) {
	c.mux.Lock()
	if c.m[name] > 0 {
		c.m[name]--
	}
	c.mux.Unlock()
} // end func Counter.Inc

func (c *COUNTER) Set(name string, val uint64) {
	c.mux.Lock()
	if val <= 0 {
		c.m[name] = 0
	} else {
		c.m[name] = val
	}
	c.mux.Unlock()
} // end func Counter.Set

// EOF mongodb_storage.go : package mongostorage

