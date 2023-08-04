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
)

// CONSTANTS comments written by AI.

// DefaultMongoUri is the default MongoDB connection string used when no URI is provided.
const DefaultMongoUri string = "mongodb://localhost:27017"

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

// NOCOMP represents the value indicating no compression for articles.
const NOCOMP int = 0

// GZIP_enc represents the value indicating GZIP compression for articles.
const GZIP_enc int = 1

// ZLIB_enc represents the value indicating ZLIB compression for articles.
const ZLIB_enc int = 2

// Note: The default values provided above are recommended for most use cases.
// However, these values can be adjusted according to your specific requirements.
// For instance, increasing the DefaultInsWorker value may help improve insert performance when dealing with a high volume of articles.
// Similarly, changing the DefaultDelWorker value may optimize delete operations for different workloads.
// Increasing the DefaultGetWorker value may help improve read performance when dealing with a high volume of read requests.
// Be cautious when modifying these settings, as improper adjustments might lead to suboptimal performance or resource utilization.
var (
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
) // end var

// iStop_Worker stops the worker of the specified type and returns the worker's maximum ID.
// Parameters:
// - wType: A string indicating the type of worker to stop.
// - Possible values are "reader", "delete", or "insert".
//
// Return Value:
// The function returns an integer representing the maximum ID of the worker.
//
// Explanation:
// The iStop_Worker function is responsible for stopping a specific type of worker (reader, delete, or insert)
// and retrieving the worker's maximum ID before stopping it. The function uses channels to communicate with
// the worker goroutines and control their termination.
// The function uses a switch statement to identify the type of worker to stop. For each type, it reads the current
// maximum worker ID from the corresponding channel. After retrieving the ID, it immediately writes it back to the
// channel, effectively "parking" the value again, so other parts of the code can still access the ID.
// This mechanism allows the caller to stop a specific worker safely and get the worker's maximum ID for further
// processing if needed.
//
// Example Usage:
// maxID := iStop_Worker("reader")
// if wid > maxID { return // stop Worker }
//
// iStop_Worker stops the worker of the specified type and returns the worker's maximum ID.
// Parameters:
// - wType: A string indicating the type of worker to stop. Possible values are "reader", "delete", or "insert".
//
// Return Value:
// The function returns an integer representing the maximum ID of the worker.
//
// Explanation:
// iStop_Worker is responsible for stopping a specific type of worker (reader, delete, or insert)
// The function uses channels to communicate indirectly with the worker goroutines to control their termination.
// Based on the provided wType, the function reads the current maximum worker ID from the corresponding channel.
// After retrieving the ID, it immediately writes it back to the channel, effectively "parking" the value again
// to allow other parts of the code to access the ID safely.
// This mechanism ensures the caller can stop a specific worker and obtain the worker's maximum ID for further processing if needed.
//
// Example Usage:
// maxID := iStop_Worker("reader")
//
//	if wid > maxID {
//	    return // stop Worker
//	}
//
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

// updn_Set updates the worker count of the specified type and starts or stops worker goroutines accordingly.
// Parameters:
// - wType: A string indicating the type of worker to update. Possible values are "reader", "delete", or "insert".
// - maxwid: An integer representing the new worker count.
// - delBatch: An integer representing the batch size for delete workers.
// - insBatch: An integer representing the batch size for insert workers.
// - mongoUri: The MongoDB connection string.
// - mongoDatabaseName: The name of the MongoDB database to connect to.
// - mongoCollection: The name of the MongoDB collection to access.
// - mongoTimeout: The timeout value in seconds for the MongoDB connection.
// - testAfterInsert: A boolean indicating whether to perform tests after inserting articles.
//
// Explanation:
// updn_Set is used by MongoWorker_UpDn_Scaler to update the count of a specific type of worker (reader, delete, or insert).
// Based on the provided wType, the function reads the current worker count from the corresponding channel and then
// writes the new worker count (maxwid) back to the channel.
// If the new worker count (maxwid) is greater than the current worker count (oldval), the function starts new worker
// goroutines for the specified type (reader, delete, or insert) up to the new worker count.
// If the new worker count is less than or equal to the current worker count, the function stops the extra worker goroutines.
// function not written by AI.
func updn_Set(wType string, maxwid int, delBatch int, insBatch int, mongoUri string, mongoDatabaseName string, mongoCollection string, mongoTimeout int64, testAfterInsert bool) {
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
				go MongoWorker_Reader(i, mongoUri, mongoDatabaseName, mongoCollection, mongoTimeout)
			}
		case "delete":
			for i := oldval + 1; i <= maxwid; i++ {
				go MongoWorker_Delete(i, delBatch, mongoUri, mongoDatabaseName, mongoCollection, mongoTimeout)
			}
		case "insert":
			for i := oldval + 1; i <= maxwid; i++ {
				go MongoWorker_Insert(i, insBatch, mongoUri, mongoDatabaseName, mongoCollection, mongoTimeout, testAfterInsert)
			}
		default:
			log.Printf("Error updn_Set unknown Wtype=%s", wType)
		} // end switch wType
	}
	log.Printf("$$ updn_Set wType=%s oldval=%d maxwid=%d", wType, oldval, maxwid)
} // end func updn_Set

// MongoWorker_UpDn_Scaler runs in the background and listens on channels for up/down requests to start/stop workers.
// Parameters:
// - getWorker: An integer representing the initial number of reader workers to start with.
// - delWorker: An integer representing the initial number of delete workers to start with.
// - insWorker: An integer representing the initial number of insert workers to start with.
//
// Explanation:
// MongoWorker_UpDn_Scaler is responsible for managing the scaling of worker goroutines based on up/down requests.
// It listens to UpDn_*_Worker_chan channels to receive requests for starting or stopping specific types of workers.
// The function uses updn_Set to update the worker counts accordingly, which effectively starts or stops worker goroutines.
// This mechanism allows the application to dynamically adjust the number of worker goroutines based on the workload
// or other factors.
// Note: The function runs in the background and continues to process requests as they arrive.
//
// function not written by AI.
func MongoWorker_UpDn_Scaler(getWorker int, delWorker int, insWorker int, delBatch int, insBatch int, mongoUri string, mongoDatabaseName string, mongoCollection string, mongoTimeout int64, testAfterInsert bool) { // <-- needs load inital values
	// load initial values into channels
	stop_reader_worker_chan <- getWorker
	stop_delete_worker_chan <- delWorker
	stop_insert_worker_chan <- insWorker
	atimeout := time.Duration(time.Second * 5)

	// The anonymous goroutine periodically checks the UpDn_StopAll_Worker_chan channel for messages.
	// If a "true" value is received, it sends "false" to all UpDn_*_Worker channels
	//   (UpDn_Reader_Worker_chan, UpDn_Delete_Worker_chan, and UpDn_Insert_Worker_chan)
	// to signal the worker goroutines to stop gracefully.
	// If a "false" value is received, no action is taken.
	// The goroutine also logs a message every 5 seconds to indicate that it is alive and running.
	// This functionality allows dynamically scaling the number of worker goroutines based on received up/down requests
	// while keeping track of their status and allowing controlled termination.
	go func() {
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
	}()
	time.Sleep(time.Second / 1000)

	// The anonymous goroutine continuously listens to the UpDn_Reader_Worker_chan channel for messages.
	// If a "true" value is received, it increments the getWorker variable to increase the number of reader worker goroutines.
	// If a "false" value is received, it decrements getWorker to decrease the number of reader worker goroutines.
	// After processing the message, the goroutine calls the updn_Set function to update the reader worker count with the new value (getWorker).
	// The goroutine also logs a message every 5 seconds to indicate that it is alive and running.
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
					getWorker--
				} // end switch retbool
				updn_Set(wType, getWorker, -1, -1, mongoUri, mongoDatabaseName, mongoCollection, mongoTimeout, testAfterInsert)
			} // end select
		} // end for
	}(getWorker, "reader")
	time.Sleep(time.Second / 1000)

	// The anonymous goroutine continuously listens to the UpDn_Delete_Worker_chan channel for messages.
	// If a "true" value is received, it increments the delWorker variable to increase the number of delete worker goroutines.
	// If a "false" value is received, it decrements delWorker to decrease the number of delete worker goroutines.
	// After processing the message, the goroutine calls the updn_Set function to update the delete worker count with the new value (delWorker).
	// The goroutine also logs a message every 5 seconds to indicate that it is alive and running.
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
					delWorker--
				} // end switch retbool
				updn_Set(wType, delWorker, delBatch, -1, mongoUri, mongoDatabaseName, mongoCollection, mongoTimeout, testAfterInsert)
			} // end select
		} // end for
	}(delWorker, "delete")
	time.Sleep(time.Second / 1000)

	// The anonymous goroutine continuously listens to the UpDn_Insert_Worker_chan channel for messages.
	// If a "true" value is received, it increments the insWorker variable to increase the number of insert worker goroutines.
	// If a "false" value is received, it decrements insWorker to decrease the number of insert worker goroutines.
	// After processing the message, the goroutine calls the updn_Set function to update the insert worker count with the new value (insWorker).
	// The goroutine also logs a message every 5 seconds to indicate that it is alive and running.
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
					insWorker--
				} // end switch retbool
				updn_Set(wType, insWorker, -1, insBatch, mongoUri, mongoDatabaseName, mongoCollection, mongoTimeout, testAfterInsert)
			} // end select
		} // end for
	}(insWorker, "insert")
	time.Sleep(time.Second / 1000)

} // end func MongoWorker_UpDn_Scaler

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
	//Newsgroups    []string `bson:"newsgroups"`
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
//     If an error occurs during the retrieval process, the channel will remain open but receive a nil slice.
//     If the Msgidhashes slice is empty or nil, the channel will receive an empty slice ([]*MongoArticle{}).
type MongoReadRequest struct {
	Msgidhashes []*string
	RetChan     chan []*MongoArticle
} // end type MongoReadRequest struct

// MongoDeleteRequest represents a delete request for deleting articles from MongoDB.
// It contains the following fields:
// - Msgidhashes: A slice of messageIDHashes for which articles are requested to be deleted.
// - RetChan: A channel to receive the deleted articles as []*MongoArticle.
// The deleted articles will be sent through this channel upon successful deletion.
// If an error occurs during the deletion process, the channel will remain open but receive a nil slice.
// If the Msgidhashes slice is empty or nil, the channel will receive an empty slice
type MongoDeleteRequest struct {
	Msgidhashes []string
	RetChan     chan []*MongoArticle
} // end type MongoDeleteRequest struct

// Load_MongoDB initializes the MongoDB storage backend with the specified configuration parameters.
// It takes the following parameters:
// - mongoUri: The MongoDB connection string. Replace 'your-mongodb-uri' with your actual MongoDB URI.
// - mongoDatabaseName: The name of the MongoDB database to connect to. If empty, the default value is used.
// - mongoCollection: The name of the MongoDB collection to access. If empty, the default value is used.
// - mongoTimeout: The timeout value in seconds for the connection. If 0, the default value is used.
// - delWorker: The number of MongoDB delete workers to create. If 0 or less, the default value is used.
// - delQueue: The size of the delete queue for holding messageIDHashes to be deleted. If 0 or less, the default value is used.
// - delBatch: The number of Msgidhashes a DeleteWorker will cache before deleting to batch into one process. If 0 or less, the default value is used.
// - insWorker: The number of MongoDB insert workers to create. If 0 or less, the default value is used.
// - insQueue: The size of the insert queue for holding MongoArticle objects to be inserted. If 0 or less, the default value is used.
// - insBatch: The number of articles an InsertWorker will cache before inserting to batch into one process. If 0 or less, the default value is used.
// - getWorker: The number of MongoDB reader workers to create. If 0 or less, the default value is used.
// - getQueue: The size of the read queue for holding MongoReadRequest objects to be processed. If 0 or less, the default value is used.
// - testAfterInsert: A boolean indicating whether to perform tests after inserting articles.
//
// The function initializes channels for delete, insert, and read operations based on the provided queue sizes.
// It creates multiple worker goroutines for delete, insert, and read operations based on the provided worker counts.
// Each worker goroutine operates independently and handles operations concurrently.
// The worker goroutines handle delete, insert, and read requests from their respective queues.
// The delete workers execute delete operations in batches using the MongoDB bulk delete feature.
// The insert workers execute insert operations in batches using the MongoDB bulk insert feature.
// The reader workers process read requests, fetch articles from MongoDB based on the provided messageIDHashes,
// and return the articles through the provided channels.
//
// Note: The function starts the worker goroutines in separate background routines. The caller of this function should manage
// the main program's lifecycle accordingly to ensure that these background goroutines terminate gracefully when needed.
// function written by AI.
func Load_MongoDB(mongoUri string, mongoDatabaseName string, mongoCollection string, mongoTimeout int64, delWorker int, delQueue int, delBatch int, insWorker int, insQueue int, insBatch int, getQueue int, getWorker int, testAfterInsert bool) {
	// Load_MongoDB initializes the mongodb storage backend
	if delQueue <= 0 {
		delQueue = DefaultDelQueue
	}
	if delWorker <= 0 {
		delWorker = DefaultDelWorker
	}
	if delBatch <= 0 {
		delBatch = DefaultDeleteBatchSize
	}
	if insQueue <= 0 {
		insQueue = DefaultInsQueue
	}
	if insWorker <= 0 {
		insWorker = DefaultInsWorker
	}
	if insBatch <= 0 {
		insBatch = DefaultInsertBatchSize
	}
	if getQueue <= 0 {
		getQueue = DefaultGetQueue
	}
	if getWorker <= 0 {
		getWorker = DefaultGetWorker
	}

	Mongo_Delete_queue = make(chan string, delQueue)
	Mongo_Insert_queue = make(chan MongoArticle, insQueue)
	Mongo_Reader_queue = make(chan MongoReadRequest, getQueue)
	log.Printf("Load_MongoDB: Reader getQueue=%d getWorker=%d", getQueue, getWorker)
	log.Printf("Load_MongoDB: Delete delQueue=%d delWorker=%d delBatch=%d", delQueue, delWorker, delBatch)
	log.Printf("Load_MongoDB: Insert insQueue=%d insWorker=%d insBatch=%d", insQueue, insWorker, insBatch)

	MongoWorker_UpDn_Scaler(getWorker, delWorker, insWorker, delBatch, insBatch, mongoUri, mongoDatabaseName, mongoCollection, mongoTimeout, testAfterInsert)

	for i := 1; i <= getWorker; i++ {
		go MongoWorker_Reader(i, mongoUri, mongoDatabaseName, mongoCollection, mongoTimeout)
	}
	for i := 1; i <= delWorker; i++ {
		go MongoWorker_Delete(i, delBatch, mongoUri, mongoDatabaseName, mongoCollection, mongoTimeout)
	}
	for i := 1; i <= insWorker; i++ {
		go MongoWorker_Insert(i, insBatch, mongoUri, mongoDatabaseName, mongoCollection, mongoTimeout, testAfterInsert)
	}

} // end func Load_MongoDB

// ConnectMongoDB is a function responsible for establishing a connection to the MongoDB server and accessing a specific collection.
// It takes the following parameters:
// - who: A string representing the name or identifier of the calling function or worker.
// - mongoUri: The MongoDB connection string. Replace 'your-mongodb-uri' with your actual MongoDB URI.
// - mongoDatabaseName: The name of the MongoDB database to connect to. If empty, the default value is used.
// - mongoCollection: The name of the MongoDB collection to access. If empty, the default value is used.
// - mongoTimeout: The timeout value in seconds for the connection. If 0, the default value is used.
//
// The function connects to the MongoDB server using the provided connection URI and establishes a client connection.
// It sets a timeout for the connection to prevent hanging connections.
// If the connection is successful, it returns a context, a cancel function to close the connection,
// a pointer to the MongoDB client, and a pointer to the MongoDB collection.
// The caller should use the provided context and cancel function to manage the connection and clean up resources after use.
// If an error occurs during the connection process, it logs the error and returns the error to the caller.
// The caller of this function should handle any returned error appropriately.
// function written by AI.
func ConnectMongoDB(who string, mongoUri string, mongoDatabaseName string, mongoCollection string, mongoTimeout int64) (context.Context, context.CancelFunc, *mongo.Client, *mongo.Collection, error) {
	// MongoDB connection string.
	// Replace 'your-mongodb-uri' with your actual MongoDB URI.
	if mongoUri == "" {
		mongoUri = DefaultMongoUri
	}
	if mongoDatabaseName == "" {
		mongoDatabaseName = DefaultMongoDatabaseName
	}
	if mongoCollection == "" {
		mongoCollection = DefaultMongoCollection
	}
	if mongoTimeout == 0 {
		mongoTimeout = DefaultMongoTimeout
	}
	// Connect to MongoDB.
	client, err := mongo.NewClient(options.Client().ApplyURI(mongoUri))
	if err != nil {
		log.Printf("MongoDB Error creating MongoDB client: %v", err)
		return nil, nil, nil, nil, err
	}

	// Set a timeout for the connection.
	newTimeout := time.Second * time.Duration(mongoTimeout)
	deadline := time.Now().Add(newTimeout)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	err = client.Connect(ctx)
	if err != nil {
		log.Printf("MongoDB Error connecting to MongoDB: %v", err)
		cancel()
		return nil, nil, nil, nil, err
	}

	// Access the MongoDB collection.
	collection := client.Database(mongoDatabaseName).Collection(mongoCollection)

	log.Printf("-> ConnectMongoDB who=%s", who)
	return ctx, cancel, client, collection, err
} // end func ConnectMongoDB

// DisConnectMongoDB is a function responsible for disconnecting from the MongoDB server.
// It takes the following parameters:
// - who: A string representing the name or identifier of the calling function or worker.
// - ctx: The context used to manage the connection and perform the disconnect operation.
// - client: A pointer to the MongoDB client instance that needs to be disconnected.
//
// The function attempts to disconnect from the MongoDB server using the provided client and context.
// If the disconnection is successful, it logs the disconnection message.
// If an error occurs during disconnection, it logs the error and returns the error.
// The caller of this function should handle any returned error appropriately.
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

// MongoWorker_Insert is a goroutine function responsible for processing incoming insert requests from the 'Mongo_Insert_queue'.
// It inserts articles into a MongoDB collection based on the articles provided in the 'Mongo_Insert_queue'.
// The function continuously listens for insert requests and processes them until the 'Mongo_Insert_queue' is closed.
//
// The function takes the following parameters:
// - wid: An integer representing the worker ID for identification purposes.
// - mongoUri: The MongoDB URI used to connect to the MongoDB server.
// - mongoDatabaseName: The name of the MongoDB database where the articles are to be stored.
// - mongoCollection: The name of the MongoDB collection where the articles are to be stored.
// - mongoTimeout: The timeout value (in seconds) for the MongoDB operations.
// - testAfterInsert: A boolean flag to indicate whether to perform article existence checks after insertions (for testing purposes).
//
// The MongoWorker_Insert initializes a MongoDB client and collection and continuously waits for incoming insert requests.
// It accumulates the articles to be inserted until the number of articles reaches the limit set by 'cap(Mongo_Insert_queue)'.
// Alternatively, it will perform the insert operation if a timeout occurs (after 5 seconds) or if the 'Mongo_Insert_queue' is closed.
//
// The function uses exponential backoff with jitter to retry connecting to the MongoDB server in case of connection errors.
// Once the 'Mongo_Insert_queue' is closed, the function performs disconnection from the MongoDB server and terminates.
//
// If the 'testAfterInsert' flag is set to true, the function will perform article existence checks after each insertion.
// This check verifies whether the article with the given MessageIDHash exists in the collection after the insertion.
// The check logs the results of the existence test for each article.
// function partly written by AI.
func MongoWorker_Insert(wid int, batchsize int, mongoUri string, mongoDatabaseName string, mongoCollection string, mongoTimeout int64, testAfterInsert bool) {
	if wid <= 0 {
		log.Printf("Error MongoWorker_Insert wid <= 0")
		return
	}
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
		ctx, cancel, client, collection, err = ConnectMongoDB(who, mongoUri, mongoDatabaseName, mongoCollection, mongoTimeout)
		if err != nil {
			attempts++
			log.Printf("MongoDB Error MongoWorker_Insert ConnectMongoDB err='%v'", err)
			time.Sleep(time.Second * calculateExponentialBackoff(attempts))
			continue
		}
		break
	}
	timeOutTime := time.Duration(time.Second * 3)
	timeout := time.After(timeOutTime)
	articles := []*MongoArticle{}
	is_timeout := false
	var diff int64
	var last_insert int64
forever:
	for {
		do_insert := false
		len_arts := len(articles)
		if len_arts == batchsize || is_timeout {
			diff = utils.UnixTimeSec() - last_insert

			if len_arts >= batchsize {
				do_insert = true
			} else if is_timeout && len_arts > 0 && diff > mongoTimeout {
				do_insert = true
			}
			if is_timeout {
				is_timeout = false
			}

			if do_insert {
				log.Printf("Pre-Ins Many msgidhashes=%d", len_arts)
				ctx, cancel = extendContextTimeout(ctx, cancel, mongoTimeout)
				if err := MongoInsertManyArticles(ctx, collection, articles); err != nil {
					reboot = true
				}
				if testAfterInsert {
					for _, article := range articles {
						if retbool, err := checkIfArticleExistsByMessageIDHash(ctx, collection, article.MessageIDHash); retbool {
							// The article with the given hash exists.
							log.Printf("article exists: %s", *article.MessageIDHash)
						} else if err != nil {
							log.Printf("Error checkIfArticleExistsByMessageIDHash: %s err %v", *article.MessageIDHash, err)
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
			articles = append(articles, &article)
			if len(articles) >= batchsize {
				break insert_queue
			}
		case <-timeout:
			is_timeout = true
			maxID := iStop_Worker("insert")
			if wid > maxID {
				log.Printf("-- Stopping %s", who)
				break forever // stop Worker
			}
			timeout = time.After(timeOutTime)
			//log.Printf("MongoWorker_Insert alive hashs=%d", len(msgidhashes))
		} // end select insert_queue
	} // end for forever
	if len(articles) > 0 {
		MongoInsertManyArticles(ctx, collection, articles)
	}
	DisConnectMongoDB(who, ctx, client)
	log.Printf("xx End %s reboot=%t", who, reboot)
	if reboot {
		go MongoWorker_Insert(wid, batchsize, mongoUri, mongoDatabaseName, mongoCollection, mongoTimeout, testAfterInsert)
	}
} // end func MongoWorker_Insert

// MongoWorker_Delete is a goroutine function responsible for processing incoming delete requests from the 'Mongo_Delete_queue'.
// It deletes articles from a MongoDB collection based on the given MessageIDHashes provided in the 'Mongo_Delete_queue'.
// The function continuously listens for delete requests and processes them until the 'Mongo_Delete_queue' is closed.
//
// The function takes the following parameters:
// - wid: An integer representing the worker ID for identification purposes.
// - mongoUri: The MongoDB URI used to connect to the MongoDB server.
// - mongoDatabaseName: The name of the MongoDB database where the articles are stored.
// - mongoCollection: The name of the MongoDB collection where the articles are stored.
// - mongoTimeout: The timeout value (in seconds) for the MongoDB operations.
//
// The MongoWorker_Delete initializes a MongoDB client and collection, and continuously waits for incoming delete requests.
// It accumulates the MessageIDHashes to be deleted until the number of hashes reaches the limit set by 'cap(Mongo_Delete_queue)'.
// Alternatively, it will perform the delete operation if a timeout occurs (after 5 seconds) or if the 'Mongo_Delete_queue' is closed.
//
// The function uses exponential backoff with jitter to retry connecting to the MongoDB server in case of connection errors.
// Once the 'Mongo_Delete_queue' is closed, the function performs disconnection from the MongoDB server and terminates.
//
// Note: If the 'limit' is set to 1, the function will delete articles one by one, processing individual delete requests from the queue.
// Otherwise, it will delete articles in bulk based on the accumulated MessageIDHashes.
// function partly written by AI.
func MongoWorker_Delete(wid int, batchsize int, mongoUri string, mongoDatabaseName string, mongoCollection string, mongoTimeout int64) {
	if wid <= 0 {
		log.Printf("Error MongoWorker_Delete wid <= 0")
		return
	}
	who := fmt.Sprintf("MongoWorker_Delete#%d", wid)
	log.Printf("++ Start %s", who)
	var ctx context.Context
	var cancel context.CancelFunc
	var client *mongo.Client
	var collection *mongo.Collection
	var err error
	attempts := 0
	for {
		ctx, cancel, client, collection, err = ConnectMongoDB(who, mongoUri, mongoDatabaseName, mongoCollection, mongoTimeout)
		if err != nil {
			attempts++
			log.Printf("MongoDB Error MongoWorker_Insert ConnectMongoDB err='%v'", err)
			time.Sleep(time.Second * calculateExponentialBackoff(attempts))
			continue
		}
		break
	}
	timeOutTime := time.Duration(time.Second * 3)
	timeout := time.After(timeOutTime)
	msgidhashes := []string{}
	is_timeout := false
	var diff int64
	var last_delete int64
forever:
	for {
		len_hashs := len(msgidhashes)
		do_delete := false
		if len_hashs == batchsize || is_timeout {
			diff = utils.UnixTimeSec() - last_delete
			if len_hashs >= batchsize {
				do_delete = true
			} else if is_timeout && len_hashs > 0 && diff > mongoTimeout {
				do_delete = true
			}
			if is_timeout {
				is_timeout = false
			}
		} // check if do_delete

		if do_delete {
			log.Printf("Pre-Del Many msgidhashes=%d", len_hashs)
			ctx, cancel = extendContextTimeout(ctx, cancel, mongoTimeout)
			MongoDeleteManyArticles(ctx, collection, msgidhashes)
			msgidhashes = []string{}
			last_delete = utils.UnixTimeSec()
		} else {
			//log.Printf("!do_insert len_hashs=%d is_timeout=%t last=%d", len_hashs, is_timeout, utils.UnixTimeSec() - last_insert)
		}
	select_delete_queue:
		select {
		case msgidhash, ok := <-Mongo_Delete_queue:
			if !ok {
				log.Printf("__ Quit %s", who)
				break forever
			}
			if batchsize == 1 { // deletes articles one by one
				for messageIDHash := range Mongo_Delete_queue {
					log.Printf("Pre-Del One msgidhash='%s'", msgidhash)
					ctx, cancel = extendContextTimeout(ctx, cancel, mongoTimeout)
					err := deleteArticlesByMessageIDHash(ctx, collection, messageIDHash)
					if err != nil {
						log.Printf("MongoDB Error deleting messageIDHash=%s err='%v'", messageIDHash, err)
						continue
					}
					log.Printf("MongoDB Deleted messageIDHash=%s", messageIDHash)
				}
			} else {
				msgidhashes = append(msgidhashes, msgidhash)
				//log.Printf("Append Del Worker: msgidhash='%s' to msgidhashes=%d", msgidhash, len(msgidhashes))
			}
			if len(msgidhashes) >= batchsize {
				break select_delete_queue
			}
		case <-timeout:
			is_timeout = true
			maxID := iStop_Worker("delete")
			if wid > maxID {
				log.Printf("-- Stopping %s", who)
				break forever // stop Worker
			}
			timeout = time.After(timeOutTime)
			//log.Printf("MongoWorker_Delete alive hashs=%d", len(msgidhashes))
			//break select_delete_queue
		} // end select delete_queue
	} // end for forever
	if len(msgidhashes) > 0 {
		MongoDeleteManyArticles(ctx, collection, msgidhashes)
	}
	DisConnectMongoDB(who, ctx, client)
	log.Printf("xx End %s", who)
} // end func MongoWorker_Delete

// MongoWorker_Reader is a goroutine function responsible for processing incoming read requests from the 'Mongo_Reader_queue'.
// It reads articles from a MongoDB collection based on the given MessageIDHashes provided in the 'readreq' parameter.
// The function continuously listens for read requests and processes them until the 'Mongo_Reader_queue' is closed.
//
// The function takes the following parameters:
// - wid: An integer representing the worker ID for identification purposes.
// - mongoUri: The MongoDB URI used to connect to the MongoDB server.
// - mongoDatabaseName: The name of the MongoDB database where the articles are stored.
// - mongoCollection: The name of the MongoDB collection where the articles are stored.
// - mongoTimeout: The timeout value (in seconds) for the MongoDB operations.
//
// The MongoWorker_Reader initializes a MongoDB client and collection, and continuously waits for incoming read requests.
// When a read request is received, it reads articles from the collection for the specified MessageIDHashes using the 'readArticlesByMessageIDHashes' function.
// If any error occurs during the retrieval, the function logs the error and continues to the next read request.
//
// The retrieved articles are then passed to the corresponding return channel in the read request ('readreq.RetChan') if it is provided.
// The return channel allows the caller to receive the articles asynchronously.
//
// The function uses exponential backoff with jitter to retry connecting to the MongoDB server in case of connection errors.
// Once the 'Mongo_Reader_queue' is closed, the function performs disconnection from the MongoDB server and terminates.
// function partly written by AI.
func MongoWorker_Reader(wid int, mongoUri string, mongoDatabaseName string, mongoCollection string, mongoTimeout int64) {
	if wid <= 0 {
		log.Printf("Error MongoWorker_Reader wid <= 0")
		return
	}
	who := fmt.Sprintf("MongoWorker_Reader#%d", wid)
	log.Printf("++ Start %s", who)
	var ctx context.Context
	var cancel context.CancelFunc
	var client *mongo.Client
	var collection *mongo.Collection
	var err error
	attempts := 0
	for {
		ctx, cancel, client, collection, err = ConnectMongoDB(who, mongoUri, mongoDatabaseName, mongoCollection, mongoTimeout)
		if err != nil {
			attempts++
			log.Printf("MongoDB Error MongoWorker_Reader ConnectMongoDB err='%v'", err)
			time.Sleep(time.Second * calculateExponentialBackoff(attempts))
			continue
		}
		break
	}
	timeOutTime := time.Duration(time.Second * 1)
	timeout := time.After(timeOutTime)
	// Process incoming read requests forever.
forever:
	for {
		select {
		case readreq, ok := <-Mongo_Reader_queue:
			if !ok {
				log.Printf("__ Quit %s", who)
				break forever
			}
			log.Printf("MongoWorker_Reader %d process readreq", wid)

			ctx, cancel = extendContextTimeout(ctx, cancel, mongoTimeout)
			// Read articles for the given msgidhashes.
			articles, err := readArticlesByMessageIDHashes(ctx, collection, readreq.Msgidhashes)
			if err != nil {
				log.Printf("Error reading articles: %v", err)
				continue
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
			maxID := iStop_Worker("reader")
			if wid > maxID {
				log.Printf("-- Stopping %s", who)
				break forever
			}
			timeout = time.After(timeOutTime)
			//log.Printf("MongoWorker_Reader alive hashs=%d", len(msgidhashes))
			//break reader_queue
		} // end select
	}
	DisConnectMongoDB(who, ctx, client)
	log.Printf("xx End %s", who)
} // end func MongoWorker_Reader

// MongoInsertOneArticle is a function that inserts a single article into a MongoDB collection.
//
// It takes a MongoDB context ('ctx'), a reference to the MongoDB collection ('collection'),
// and a pointer to the 'MongoArticle' object ('article') that represents the article to be inserted.
//
// The function uses the MongoDB 'InsertOne' operation to insert a single document into the collection.
// It passes the 'article' pointer to the 'InsertOne' method, and MongoDB handles the insertion based on the data in the 'MongoArticle' object.
//
// If the insertion is successful, the function returns 'nil' error.
// If an error occurs during the insertion, the function logs the error and returns the error instance to the caller.
//
// Note: The function assumes that the 'MongoArticle' struct is used to represent articles and contains the necessary data for insertion.
// Ensure that the 'article' pointer points to a valid 'MongoArticle' object with all the required fields populated before calling this function.
// function written by AI.
func MongoInsertOneArticle(ctx context.Context, collection *mongo.Collection, article *MongoArticle) error {
	_, err := collection.InsertOne(ctx, article)
	if err != nil {
		log.Printf("Error collection.InsertOne err='%v'", err)
	}
	return err
} // end func MongoInsertOneArticle

// MongoInsertManyArticles is a function that performs a bulk insert of multiple articles into a MongoDB collection.
//
// It takes a MongoDB context ('ctx') and a reference to the MongoDB collection ('collection') where the articles will be inserted.
// The articles to be inserted are provided as a slice of 'MongoArticle' objects ('articles').
//
// The function uses the MongoDB 'InsertMany' operation to insert multiple documents in a single call to the database.
// It constructs an array of 'interface{}' containing the articles to be inserted, and sets the 'ordered' option to 'false' to allow
// non-sequential insertion of documents, even if some of them have duplicate '_id' values.
// If a duplicate '_id' (message ID hash) is encountered during the insert operation, MongoDB will continue inserting other documents,
// and duplicates will be ignored. The first occurrence of each unique '_id' will be inserted, and subsequent occurrences will be skipped.
//
// After the insertion operation is completed, the function checks if the number of successfully inserted documents matches the number of articles.
// If all articles are successfully inserted, it returns 'true', indicating a successful bulk insert. Otherwise, it returns 'false'.
//
// Note: The function assumes that the 'MongoArticle' struct is used to represent articles and contains the necessary data for insertion.
// The MongoDB 'InsertMany' operation may impose certain limitations on the number of documents that can be inserted in a single batch,
// so ensure that the number of articles in the 'articles' slice is within acceptable limits to avoid potential issues.
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

// MongoDeleteManyArticles is responsible for deleting multiple articles from the MongoDB collection
// based on a given set of MessageIDHashes. It performs a bulk delete operation efficiently to remove
// articles in batches, minimizing database load and improving performance.
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
// context and creating a new one with an updated timeout. This function is useful for
// ensuring that long-running operations do not exceed a specified timeout.
//
// Parameters:
//   - ctx: The original context that needs its deadline extended.
//   - cancel: The cancel function associated with the original context.
//   - mongoTimeout: The new timeout value in seconds for the updated context.
//
// Returns:
//   - context.Context: The updated context with an extended deadline.
//   - context.CancelFunc: The cancel function associated with the updated context.
//
// function written by AI.
func extendContextTimeout(ctx context.Context, cancel context.CancelFunc, mongoTimeout int64) (context.Context, context.CancelFunc) {
	//log.Printf("extendContextTimeout")
	cancel()
	newTimeout := time.Second * time.Duration(mongoTimeout)
	deadline := time.Now().Add(newTimeout)
	ctx, cancel = context.WithDeadline(context.Background(), deadline)
	return ctx, cancel
} // end func extendContextTimeout

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
// It takes the MongoDB context "ctx", the MongoDB collection "collection", and the MessageIDHash of the article to retrieve.
// It returns a pointer to the retrieved MongoArticle object and an error if any.
// If the article with the given MessageIDHash does not exist, it returns nil and nil error.
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
			return nil, nil
		}
		// Return other errors as they indicate a problem with the query.
		return nil, result.Err()
	}

	// Decode the article from the BSON representation to a MongoArticle object.
	var article MongoArticle
	if err := result.Decode(&article); err != nil {
		return nil, err
	}

	return &article, nil
} // end func retrieveArticleByMessageIDHash

// readArticlesByMessageIDHashes is a function that retrieves articles from the MongoDB collection based on a list of
// MessageIDHashes. It takes a MongoDB context 'ctx', a 'collection' pointer representing the MongoDB collection
// to query, and a slice of 'msgidhashes' containing the MessageIDHashes to look for in the database.
// The function filters the collection using the provided MessageIDHashes and retrieves the matching articles.
// It returns a slice of pointers to MongoArticle objects containing the retrieved articles and their corresponding
// 'found' status, indicating whether each article was found in the database. If an article is not found in the database,
// a new empty MongoArticle object with the specified MessageIDHash is added to the result slice.
// function written by AI.
func readArticlesByMessageIDHashes(ctx context.Context, collection *mongo.Collection, msgidhashes []*string) ([]*MongoArticle, error) {
	// Filter to find the articles with the given MessageIDHashes.
	filter := bson.M{"_id": bson.M{"$in": msgidhashes}}

	// Find the articles in the collection.
	cursor, err := collection.Find(ctx, filter)
	if err != nil {
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
		return nil, err
	}

	return articles, nil
} // end func readArticlesByMessageIDHashes

// sliceContains checks if a given target string exists in the provided slice of strings.
// It returns true if the target string is found in the slice, otherwise, it returns false.
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
// It iterates through the slice and compares each element to the target pointer.
// If the target and any element in the slice are not nil and have the same string value,
// the function returns true indicating that the target is present in the slice.
// Otherwise, it returns false.
// function written by AI.
func isPStringInSlice(slice []*string, target *string) bool {
	for _, s := range slice {
		if s != nil && target != nil && *s == *target {
			return true
		}
	}
	return false
} // end func isPStringInSlice

// RetrieveHeadByMessageIDHash is a function that retrieves the "Head" data of an article from the MongoDB collection
// based on its MessageIDHash. It takes a MongoDB context 'ctx', a 'collection' pointer representing the MongoDB collection
// to query, and the 'messageIDHash' string that uniquely identifies the article.
//
// The function filters the collection using the provided MessageIDHash and retrieves the corresponding article's "Head" field,
// which contains the article's header information. If the article is found, it returns the "Head" data as a byte slice.
// If the article does not exist in the collection, the function returns nil and no error.
// If any other errors occur during the query or decoding process, they will be returned as errors.
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

// RetrieveBodyByMessageIDHash is a function that retrieves the "Body" data of an article from the MongoDB collection
// based on its MessageIDHash. It takes a MongoDB context 'ctx', a 'collection' pointer representing the MongoDB collection
// to query, and the 'messageIDHash' string that uniquely identifies the article.
//
// The function filters the collection using the provided MessageIDHash and retrieves the corresponding article's "Body" field,
// which contains the article's content. If the article is found, it returns the "Body" data as a byte slice.
// If the article does not exist in the collection, the function returns nil and no error.
// If any other errors occur during the query or decoding process, they will be returned as errors.
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

// RetrieveBodyByMessageIDHash is a function that retrieves the "Body" data of an article from the MongoDB collection
// based on its MessageIDHash. It takes a MongoDB context 'ctx', a 'collection' pointer representing the MongoDB collection
// to query, and the 'messageIDHash' string that uniquely identifies the article.
//
// The function filters the collection using the provided MessageIDHash and retrieves the corresponding article's "Body" field,
// which contains the article's content. If the article is found, it returns the "Body" data as a byte slice.
// If the article does not exist in the collection, the function returns nil and no error.
// If any other errors occur during the query or decoding process, they will be returned as errors.
// function written by AI.
func checkIfArticleExistsByMessageIDHash(ctx context.Context, collection *mongo.Collection, messageIDHash *string) (bool, error) {
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
} // end func checkIfArticleExistsByMessageIDHash

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
//
// The 'algo' parameter specifies the compression algorithm to be used, and it should be one of the constants 'GZIP_enc' or 'ZLIB_enc'.
// If the provided 'algo' value is not one of the supported compression algorithms, the function returns an error with an appropriate message.
//
// The function utilizes the 'gzip' or 'zlib' packages from the Go standard library to perform compression.
// It creates a new writer for the specified algorithm, writes the input data, flushes the writer, and finally closes it to obtain the compressed data.
// The compressed data is returned as a new byte slice.
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
//
// The 'algo' parameter specifies the compression algorithm used to compress the data, and it should be one of the constants 'GZIP_enc' or 'ZLIB_enc'.
// If the provided 'algo' value is not one of the supported compression algorithms, the function returns an error with an appropriate message.
//
// The function utilizes the 'gzip' or 'zlib' packages from the Go standard library to perform decompression.
// It creates a new reader for the specified algorithm, reads the input data from the provided byte slice, and then closes the reader to release resources.
// The decompressed data is returned as a new byte slice.
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
//
// The function uses exponential backoff with jitter to increase the backoff duration for each retry attempt.
// The 'attempt' parameter specifies the current attempt number, starting from 1 for the first attempt.
//
// It uses a base backoff duration ('backoffBase') of 100 milliseconds, and a backoff factor ('backoffFactor') of 2.
// The backoff duration is calculated as 'backoffFactor' raised to the power of 'attempt-1' multiplied by the 'backoffBase'.
// This results in a gradually increasing backoff duration for each attempt.
//
// The function also applies a maximum backoff duration ('maxbackoff') of 30,000 milliseconds (30 seconds) to prevent excessively long delays.
// If the calculated backoff duration exceeds the maximum, it is capped at the maximum value.
//
// To add jitter and prevent all clients from retrying simultaneously, the function introduces some random jitter by adding a random duration
// (up to half of the 'backoffBase') to the calculated backoff duration.
//
// The final backoff duration is returned as a 'time.Duration' value, representing the total time to wait before the next retry attempt.
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

// randomUpDN periodically sends random up/down (true/false) signals to the worker channels.
// This function generates two random integers: arandA and arandB.
// It then interprets the values of arandA and arandB to determine which worker channel to send the signal to.
// The function uses a switch statement to decide which worker channel to use based on the random values.
// If arandA is 1, it sets sendbool to true; otherwise, it remains false.
// Depending on the value of arandB, the function sends the sendbool value to one of four worker channels:
// - If arandB is 0, it sends the signal to the "reader" worker channel.
// - If arandB is 1, it sends the signal to the "delete" worker channel.
// - If arandB is 2, it sends the signal to the "insert" worker channel.
// - If arandB is 3, it sends the signal to the "StopAll" worker channel, which will stop all worker goroutines if sendbool is true.
// The function then repeats this process in an infinite loop with a 5 second sleep between iterations.
// The purpose of this function is to simulate random up/down requests to control the worker
// function not written by AI.
// ./mongodbtest -randomUpDN -test-num 0
func MongoWorker_RandomUpDN() {
	isleep := 5
	log.Print("Start mongostorage.MongoWorker_RandomUpDN")
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
			log.Printf("~~ randomUpDN sending %t to UpDn_Reader_Worker_chan", sendbool)
			UpDn_Reader_Worker_chan <- sendbool
		case 1:
			//wType = "delete"
			log.Printf("~~ randomUpDN sending %t to UpDn_Delete_Worker_chan", sendbool)
			UpDn_Delete_Worker_chan <- sendbool
		case 2:
			//wType = "insert"
			log.Printf("~~ randomUpDN sending %t to UpDn_Insert_Worker_chan", sendbool)
			UpDn_Insert_Worker_chan <- sendbool
		case 3:
			//wType = "StopAll"
			log.Printf("~~ randomUpDN sending %t to UpDn_StopAll_Worker_chan", sendbool)
			UpDn_StopAll_Worker_chan <- sendbool
		default:
		}
	}
} // end func MongoWorker_RandomUpDN

// EOF mongodb_storage.go
