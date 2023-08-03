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
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"io/ioutil"
	"log"
	"math/rand"
	"time"
)

const (
	DefaultMongoUri          string = "mongodb://localhost:27017"
	DefaultMongoDatabaseName string = "nntp"
	DefaultMongoCollection   string = "articles"
	DefaultMongoTimeout      int64  = 15
	// DefaultDelQueue: sets objects a DeleteWorker will queue before processing
	DefaultDelQueue  int = 2
	DefaultDelWorker int = 1 // start this many DeleteWorker
	// DefaultInsQueue: sets objects a InsertWorker will queue before processing
	DefaultInsQueue  int = 2
	DefaultInsWorker int = 1 // start this many InsertWorker
	// DefaultGetQueue: sets total queue length for all ReaderWorker
	DefaultGetQueue  int = 2
	DefaultGetWorker int = 1 // start this many ReaderWorker
	NOCOMP           int = 0
	GZIP_enc         int = 1
	ZLIB_enc         int = 2
)

var (
	Mongo_Delete_queue chan string
	Mongo_Insert_queue chan MongoArticle
	Mongo_Reader_queue chan MongoReadRequest
)

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

type MongoReadReqReturn struct {
	Articles []*MongoArticle
} // end type MongoReadReqReturn struct

type MongoReadRequest struct {
	Msgidhashes []string
	RetChan     chan []*MongoArticle
} // end type MongoReadRequest struct

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

func DisConnectMongoDB(who string, ctx context.Context, client *mongo.Client) error {
	err := client.Disconnect(ctx)
	if err != nil {
		log.Printf("MongoDB Error disconnecting from MongoDB: %v", err)
		return err
	}
	log.Printf("DisConnectMongoDB who=%s", who)
	return nil
} // end func DisConnectMongoDB

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

// Insert an article into MongoDB.
func MongoInsertOneArticle(ctx context.Context, collection *mongo.Collection, article *MongoArticle) {
	_, err := collection.InsertOne(ctx, article)
	if err != nil {
		log.Fatalf("Error collection.InsertOne err='%v'", err)
	}
} // end func MongoInsertOneArticle

// Insert many articles into MongoDB.
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
		log.Fatalf("Error MongoDeleteManyArticles err='%v'", err)
		return false
	}

	log.Printf("MongoDB Deleted many=%d", result.DeletedCount)
	return result.DeletedCount == int64(len(msgidhashes))
}

func MongoDeleteManyArticles2(ctx context.Context, collection *mongo.Collection, msgidhashes []string) bool {
	// Delete all documents that match the filters
	to_delete := len(msgidhashes)
	/*
	   // Example slice of primitive.M (map) objects
	   manyfilters := []primitive.M{
	       {"messageIDHash": "abcd", "x": a},
	       {"messageIDHash": "ffff", "y": b},
	       {"messageIDHash": "ffff", "z": c},
	   }
	*/

	// Empty slice of primitive.M (map) objects
	manyfilters := []primitive.M{}

	// Create a BSON array
	bsonArray := bson.A{}

	// Add each map element to the BSON array
	for _, messageIDHash := range msgidhashes {
		bsonArray = append(bsonArray, messageIDHash)
	}
	// Create a BSON document to hold the array
	bsonDoc := bson.D{
		{"0", bsonArray},
	}

	// Convert the BSON document to bytes (marshal)
	data, err := bson.Marshal(bsonDoc)
	if err != nil {
		log.Fatal(err)
	}

	for _, messageIDHash := range msgidhashes {
		manyfilters = append(manyfilters, bson.M{"_id": messageIDHash})
	}
	result, err := collection.DeleteMany(ctx, data)
	if err != nil {
		log.Fatalf("Error MongoDeleteManyArticles err='%v'", err)
		return false
	}
	log.Printf("MongoDB Deleted many=%d", result.DeletedCount)
	return result.DeletedCount == int64(to_delete)
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

// retrieveArticleByMessageIDHash retrieves the article with the given MessageIDHash from the MongoDB collection.
// If the article exists, it returns the MongoArticle object; otherwise, it returns nil and an error.
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

// retrieveHeadByMessageIDHash retrieves the "Head" of the article with the given "messageIDHash" from the MongoDB collection.
// If the article exists, it returns the "Head" as a byte slice; otherwise, it returns nil and an error.
func retrieveHeadByMessageIDHash(ctx context.Context, collection *mongo.Collection, messageIDHash string) ([]byte, error) {
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

// retrieveBodyByMessageIDHash retrieves the "Body" of the article with the given "messageIDHash" from the MongoDB collection.
// If the article exists, it returns the "Body" as a byte slice; otherwise, it returns nil and an error.
func retrieveBodyByMessageIDHash(ctx context.Context, collection *mongo.Collection, messageIDHash string) ([]byte, error) {
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

// CompressData compresses the input data using the specified compression algorithm.
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

// DecompressData decompresses the input data using the specified compression algorithm.
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
