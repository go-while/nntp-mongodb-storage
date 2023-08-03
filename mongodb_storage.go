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

// DefaultInsQueue sets the number of objects an InsertWorker will queue before processing.
const DefaultInsQueue int = 2

// DefaultInsWorker sets the number of InsertWorker instances to start by default.
const DefaultInsWorker int = 1

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

var (
	Mongo_Delete_queue chan string
	Mongo_Insert_queue chan MongoArticle
	Mongo_Reader_queue chan MongoReadRequest
)

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

// Example Usage:
//   article := MongoArticle{
//     MessageIDHash: "hash123",
//     MessageID:     "<msgid123@example.com>",
//     Newsgroups:    []string{"comp.lang.go", "comp.lang.python"},
//     Head:          []byte("Subject: Hello, World!\nFrom: user@example.com\n"),
//     Headsize:      46,
//     Body:          []byte("This is the content of the article."),
//     Bodysize:      30,
//     Enc:           0,
//     Found:         true,
//   }
type MongoArticle struct {
	MessageIDHash string   `bson:"_id"`
	MessageID     string   `bson:"msgid"`
	Newsgroups    []string `bson:"newsgroups"`
	Head          []byte   `bson:"head"`
	Headsize      int      `bson:"hs"`
	Body          []byte   `bson:"body"`
	Bodysize      int      `bson:"bs"`
	Enc           int      `bson:"enc"`
	Found         bool
} // end type MongoArticle struct

// MongoReadReqReturn represents the return value for a read request in MongoDB.
// It contains the following field:
// - Articles: A slice of pointers to MongoArticle objects representing the fetched articles.
type MongoReadReqReturn struct {
	Articles []*MongoArticle
} // end type MongoReadReqReturn struct

// MongoReadRequest represents a read request for fetching articles from MongoDB.
// It contains the following fields:
// - Msgidhashes: A slice of messageIDHashes for which articles are requested.
// - RetChan: A channel to receive the fetched articles as []*MongoArticle.
//            The fetched articles will be sent through this channel upon successful retrieval.
//            If an error occurs during the retrieval process, the channel will remain open but receive a nil slice.
//            If the Msgidhashes slice is empty or nil, the channel will receive an empty slice ([]*MongoArticle{}).
//
// Example Usage:
//   req := MongoReadRequest{
//     Msgidhashes: []string{"hash1", "hash2", "hash3"},
//     RetChan:     make(chan []*MongoArticle),
//   }
//   Mongo_Reader_queue <- req
//   articles := <-req.RetChan
//   // Handle the fetched articles...
type MongoReadRequest struct {
	Msgidhashes []string
	RetChan     chan []*MongoArticle
} // end type MongoReadRequest struct

// Load_MongoDB initializes the MongoDB storage backend with the specified configuration parameters.
// It takes the following parameters:
// - mongoUri: The MongoDB connection string. Replace 'your-mongodb-uri' with your actual MongoDB URI.
// - mongoDatabaseName: The name of the MongoDB database to connect to. If empty, the default value is used.
// - mongoCollection: The name of the MongoDB collection to access. If empty, the default value is used.
// - mongoTimeout: The timeout value in seconds for the connection. If 0, the default value is used.
// - delWorker: The number of MongoDB delete workers to create. If 0 or less, the default value is used.
// - delQueue: The size of the delete queue for holding messageIDHashes to be deleted. If 0 or less, the default value is used.
// - insWorker: The number of MongoDB insert workers to create. If 0 or less, the default value is used.
// - insQueue: The size of the insert queue for holding MongoArticle objects to be inserted. If 0 or less, the default value is used.
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
func Load_MongoDB(mongoUri string, mongoDatabaseName string, mongoCollection string, mongoTimeout int64, delWorker int, delQueue int, insWorker int, insQueue int, getQueue int, getWorker int, testAfterInsert bool) {
	// Load_MongoDB initializes the mongodb storage backend
	if delQueue <= 0 {
		delQueue = DefaultDelQueue
	}
	if delWorker <= 0 {
		delWorker = DefaultDelWorker
	}
	if insQueue <= 0 {
		insQueue = DefaultInsQueue
	}
	if insWorker <= 0 {
		insWorker = DefaultInsWorker
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
	for i := 1; i <= delWorker; i++ {
		go MongoDeleteWorker(i, mongoUri, mongoDatabaseName, mongoCollection, mongoTimeout)
	}
	for i := 1; i <= insWorker; i++ {
		go MongoInsertWorker(i, mongoUri, mongoDatabaseName, mongoCollection, mongoTimeout, testAfterInsert)
	}
	for i := 1; i <= getWorker; i++ {
		go MongoReaderWorker(i, mongoUri, mongoDatabaseName, mongoCollection, mongoTimeout)
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

	log.Printf("ConnectMongoDB who=%s", who)
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
func DisConnectMongoDB(who string, ctx context.Context, client *mongo.Client) error {
	err := client.Disconnect(ctx)
	if err != nil {
		log.Printf("MongoDB Error disconnecting from MongoDB: %v", err)
		return err
	}
	log.Printf("DisConnectMongoDB who=%s", who)
	return nil
} // end func DisConnectMongoDB

// MongoInsertWorker is a goroutine function responsible for processing incoming insert requests from the 'Mongo_Insert_queue'.
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
// The MongoInsertWorker initializes a MongoDB client and collection and continuously waits for incoming insert requests.
// It accumulates the articles to be inserted until the number of articles reaches the limit set by 'cap(Mongo_Insert_queue)'.
// Alternatively, it will perform the insert operation if a timeout occurs (after 5 seconds) or if the 'Mongo_Insert_queue' is closed.
//
// The function uses exponential backoff with jitter to retry connecting to the MongoDB server in case of connection errors.
// Once the 'Mongo_Insert_queue' is closed, the function performs disconnection from the MongoDB server and terminates.
//
// If the 'testAfterInsert' flag is set to true, the function will perform article existence checks after each insertion.
// This check verifies whether the article with the given MessageIDHash exists in the collection after the insertion.
// The check logs the results of the existence test for each article.
func MongoInsertWorker(wid int, mongoUri string, mongoDatabaseName string, mongoCollection string, mongoTimeout int64, testAfterInsert bool) {
	who := fmt.Sprintf("MongoInsertWorker#%d", wid)
	var ctx context.Context
	var cancel context.CancelFunc
	var client *mongo.Client
	var collection *mongo.Collection
	var err error
	attempts := 0
	for {
		//ctx, cancel, client, collection, err = ConnectMongoDB(who, mongoUri, mongoDatabaseName, mongoCollection, mongoTimeout)
		ctx, cancel, client, collection, err = ConnectMongoDB(who, mongoUri, mongoDatabaseName, mongoCollection, mongoTimeout)
		if err != nil {
			attempts++
			log.Printf("MongoDB Error MongoInsertWorker ConnectMongoDB err='%v'", err)
			time.Sleep(time.Second * calculateExponentialBackoff(attempts))
			continue
		}
		break
	}
	timeout := time.After(time.Second * 5)
	limit := cap(Mongo_Insert_queue)
	articles := []*MongoArticle{}
	is_timeout := false
	stop := false
	var diff int64
	var last_insert int64
forever:
	for {
		do_insert := false
		len_arts := len(articles)
		if len_arts == limit || is_timeout || stop {
			diff = utils.UnixTimeSec() - last_insert

			if len_arts > 0 {
				do_insert = true
			}
			if do_insert && is_timeout && diff < mongoTimeout {
				do_insert = false
			}
			if !do_insert && stop && len_arts > 0 {
				do_insert = true
			}
			if !stop && is_timeout {
				is_timeout = false
				timeout = time.After(time.Second * 5)
			}

			if do_insert {
				log.Printf("Pre-Ins Many msgidhashes=%d", len_arts)
				ctx, cancel = extendContextTimeout(ctx, cancel, mongoTimeout)
				MongoInsertManyArticles(ctx, collection, articles)
				if testAfterInsert {
					for _, article := range articles {
						if retbool, err := checkIfArticleExistsByMessageIDHash(ctx, collection, article.MessageIDHash); retbool {
							// The article with the given hash exists.
							log.Printf("article exists: %s", article.MessageIDHash)
						} else if err != nil {
							log.Printf("Error checkIfArticleExistsByMessageIDHash: %s err %v", article.MessageIDHash, err)
						}
					}
				}
				last_insert = utils.UnixTimeSec()
				articles = []*MongoArticle{}
			}
			if is_timeout {
				is_timeout = false
				timeout = time.After(time.Second * 5)
			}
		}
		if stop {
			break forever
		}
	insert_queue:
		select {
		case article, ok := <-Mongo_Insert_queue:
			if !ok {
				stop = true
			} else {
				articles = append(articles, &article)
			}
			if len(articles) >= limit {
				break insert_queue
			}
		case <-timeout:
			is_timeout = true
			break insert_queue
		} // end select
	} // end for
	if len(articles) > 0 {
		MongoInsertManyArticles(ctx, collection, articles)
	}
	DisConnectMongoDB(who, ctx, client)
	log.Printf("Quit MongoInsertWorker %d", wid)
} // end func MongoInsertWorker

// MongoDeleteWorker is a goroutine function responsible for processing incoming delete requests from the 'Mongo_Delete_queue'.
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
// The MongoDeleteWorker initializes a MongoDB client and collection, and continuously waits for incoming delete requests.
// It accumulates the MessageIDHashes to be deleted until the number of hashes reaches the limit set by 'cap(Mongo_Delete_queue)'.
// Alternatively, it will perform the delete operation if a timeout occurs (after 5 seconds) or if the 'Mongo_Delete_queue' is closed.
//
// The function uses exponential backoff with jitter to retry connecting to the MongoDB server in case of connection errors.
// Once the 'Mongo_Delete_queue' is closed, the function performs disconnection from the MongoDB server and terminates.
//
// Note: If the 'limit' is set to 1, the function will delete articles one by one, processing individual delete requests from the queue.
// Otherwise, it will delete articles in bulk based on the accumulated MessageIDHashes.
func MongoDeleteWorker(wid int, mongoUri string, mongoDatabaseName string, mongoCollection string, mongoTimeout int64) {
	who := fmt.Sprintf("MongoDeleteWorker#%d", wid)
	var ctx context.Context
	var cancel context.CancelFunc
	var client *mongo.Client
	var collection *mongo.Collection
	var err error
	attempts := 0
	for {
		//ctx, cancel, client, collection, err = ConnectMongoDB(who, mongoUri, mongoDatabaseName, mongoCollection, mongoTimeout)
		ctx, cancel, client, collection, err = ConnectMongoDB(who, mongoUri, mongoDatabaseName, mongoCollection, mongoTimeout)
		if err != nil {
			attempts++
			log.Printf("MongoDB Error MongoInsertWorker ConnectMongoDB err='%v'", err)
			time.Sleep(time.Second * calculateExponentialBackoff(attempts))
			continue
		}
		break
	}
	timeout := time.After(time.Second * 5)
	limit := cap(Mongo_Delete_queue)
	msgidhashes := []string{}
	is_timeout := false
	stop := false
	var diff int64
	var last_delete int64
forever:
	for {
		len_hashs := len(msgidhashes)
		do_delete := false
		if len_hashs == limit || is_timeout || stop {
			diff = utils.UnixTimeSec() - last_delete
			if len_hashs > 0 {
				do_delete = true
			}
			if do_delete && is_timeout && diff < mongoTimeout {
				do_delete = false
			}
			if !do_delete && stop && len_hashs > 0 {
				do_delete = true
			}
			if !stop && is_timeout {
				is_timeout = false
				timeout = time.After(time.Second * 5)
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
		if stop {
			break forever
		}
	delete_queue:
		select {
		case msgidhash, ok := <-Mongo_Delete_queue:
			if !ok {
				stop = true
			}
			if limit == 1 { // deletes articles one by one
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
			if len(msgidhashes) >= limit {
				break delete_queue
			}

		case <-timeout:
			is_timeout = true
			log.Printf("MongoDeleteWorker timeout, refresh hashs=%d", len(msgidhashes))
			break delete_queue
		} // end select
		//log.Print("MongoDeleteWorker // end select")
	} // end for
	if len(msgidhashes) > 0 {
		MongoDeleteManyArticles(ctx, collection, msgidhashes)
	}
	DisConnectMongoDB(who, ctx, client)
	log.Printf("Quit MongoDeleteWorker %d", wid)
} // end func MongoDeleteWorker

// MongoReaderWorker is a goroutine function responsible for processing incoming read requests from the 'Mongo_Reader_queue'.
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
// The MongoReaderWorker initializes a MongoDB client and collection, and continuously waits for incoming read requests.
// When a read request is received, it reads articles from the collection for the specified MessageIDHashes using the 'readArticlesByMessageIDHashes' function.
// If any error occurs during the retrieval, the function logs the error and continues to the next read request.
//
// The retrieved articles are then passed to the corresponding return channel in the read request ('readreq.RetChan') if it is provided.
// The return channel allows the caller to receive the articles asynchronously.
//
// The function uses exponential backoff with jitter to retry connecting to the MongoDB server in case of connection errors.
// Once the 'Mongo_Reader_queue' is closed, the function performs disconnection from the MongoDB server and terminates.
func MongoReaderWorker(wid int, mongoUri string, mongoDatabaseName string, mongoCollection string, mongoTimeout int64) {
	who := fmt.Sprintf("MongoReaderWorker#%d", wid)
	var ctx context.Context
	var cancel context.CancelFunc
	var client *mongo.Client
	var collection *mongo.Collection
	var err error
	attempts := 0
	for {
		//ctx, cancel, client, collection, err = ConnectMongoDB(who, mongoUri, mongoDatabaseName, mongoCollection, mongoTimeout)
		ctx, cancel, client, collection, err = ConnectMongoDB(who, mongoUri, mongoDatabaseName, mongoCollection, mongoTimeout)
		if err != nil {
			attempts++
			log.Printf("MongoDB Error MongoReaderWorker ConnectMongoDB err='%v'", err)
			time.Sleep(time.Second * calculateExponentialBackoff(attempts))
			continue
		}
		break
	}
	// Process incoming read requests forever.
forever:
	for {
		select {
		case readreq, ok := <-Mongo_Reader_queue:
			if !ok {
				log.Printf("Quit MongoReaderWorker %d", wid)
				break forever
			}
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
				log.Printf("passing %d/%d read articles response to readreq.RetChan", len_got_arts, len_request)
				readreq.RetChan <- articles // MongoReadReqReturn{ Articles: articles }
			}

			// Do something with the articles, e.g., handle them or send them to another channel.

		case <-ctx.Done():
			log.Printf("MongoReaderWorker %d context canceled", wid)
			return
		}
	}
	DisConnectMongoDB(who, ctx, client)
	log.Printf("Quit MongoReaderWorker %d", wid)
} // end func MongoReaderWorker

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
func MongoInsertManyArticles(ctx context.Context, collection *mongo.Collection, articles []*MongoArticle) bool {
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
		log.Printf("Error MongoInsertManyArticles err='%v' inserted=%d", err, len(result.InsertedIDs))
	}
	if len(result.InsertedIDs) == len(articles) {
		return true
	}
	return false
} // end func MongoInsertManyArticles

// MongoDeleteManyArticles is responsible for deleting multiple articles from the MongoDB collection
// based on a given set of MessageIDHashes. It performs a bulk delete operation efficiently to remove
// articles in batches, minimizing database load and improving performance.
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
func readArticlesByMessageIDHashes(ctx context.Context, collection *mongo.Collection, msgidhashes []string) ([]*MongoArticle, error) {
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
	var founds []string
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
		if !sliceContains(founds, hash) {
			log.Printf("readArticlesByMessageIDHashes notfound hash='%s'", hash)
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
func sliceContains(slice []string, target string) bool {
	for _, item := range slice {
		if item == target {
			return true
		}
	}
	return false
} // end func sliceContains

// RetrieveHeadByMessageIDHash is a function that retrieves the "Head" data of an article from the MongoDB collection
// based on its MessageIDHash. It takes a MongoDB context 'ctx', a 'collection' pointer representing the MongoDB collection
// to query, and the 'messageIDHash' string that uniquely identifies the article.
//
// The function filters the collection using the provided MessageIDHash and retrieves the corresponding article's "Head" field,
// which contains the article's header information. If the article is found, it returns the "Head" data as a byte slice.
// If the article does not exist in the collection, the function returns nil and no error.
// If any other errors occur during the query or decoding process, they will be returned as errors.
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
func checkIfArticleExistsByMessageIDHash(ctx context.Context, collection *mongo.Collection, messageIDHash string) (bool, error) {
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
		return nil, fmt.Errorf("unsupported compression algorithm: %s", algo)
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
		return nil, fmt.Errorf("unsupported compression algorithm: %s", algo)
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

// EOF mongodb_storage.go
