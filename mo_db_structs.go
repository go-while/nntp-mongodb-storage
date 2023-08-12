package mongostorage

const (
	DefaultMongoURI          string = "mongodb://localhost:27017"
	DefaultMongoDatabaseName string = "nntp"
	DefaultMongoCollection   string = "articles"
	DefaultMongoTimeout      int64  = 15
	DefaultDelQueue          int    = 1
	DefaultDelWorker         int    = 1
	DefaultDeleteBatchSize   int    = 1
	DefaultInsQueue          int    = 1
	DefaultInsWorker         int    = 1
	DefaultInsertBatchSize   int    = 1
	DefaultGetQueue          int    = 1
	DefaultGetWorker         int    = 1
	DefaultFlushTimer        int64  = 1000
)

// Compression constants
const (
	NOCOMP    int = 0
	GZIP_enc  int = 1
	ZLIB_enc  int = 2
	FLATE_enc int = 3
)

// Note: The default values provided above are recommended for most use cases.
// However, these values can be adjusted according to your specific requirements.
// Be cautious when modifying these settings, as improper adjustments might lead to suboptimal performance or resource utilization.

var (
	Counter COUNTER
	// _queue channels handle requests for read/get, delete and insert
	Mongo_Reader_queue chan *MongoGetRequest
	Mongo_Delete_queue chan *MongoDelRequest
	Mongo_Insert_queue chan *MongoArticle
	// pass a bool [true|false] to the UpDN_ channels and workers will start or stop
	// external access via: mongostorage.UpDn_***_Worker_chan
	UpDn_StopAll_Worker_chan = make(chan bool, 1)
	UpDn_Reader_Worker_chan  = make(chan bool, 1)
	UpDn_Delete_Worker_chan  = make(chan bool, 1)
	UpDn_Insert_Worker_chan  = make(chan bool, 1)
	LOCK_UpDnScaler          = make(chan struct{}, 1)
	// internal channels to notify workers to stop
	stop_reader_worker_chan        = make(chan int, 1)
	stop_delete_worker_chan        = make(chan int, 1)
	stop_insert_worker_chan        = make(chan int, 1)
	worker_status_chan             = make(chan workerstatus, 65535)
	READER                  string = "reader"
	DELETE                  string = "delete"
	INSERT                  string = "insert"
) // end var

type workerstatus struct {
	wType  string
	status update
}

type update struct {
	Did  int
	Bad  int
	Boot bool
	Stop bool
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

	// DelBatch sets the number of MessageIDs a DeleteWorker will cache before deleting to batch into one process.
	DelBatch int

	// InsBatch sets the number of Articles an InsertWorker will cache before inserting to batch into one process.
	InsBatch int

	// FlushTimer represent the default time in milliseconds for flushing batched operations to MongoDB.
	FlushTimer int64
} // end type MongoStorageConfig

// MongoArticle represents an article stored in MongoDB.
// It contains the following fields:
// - MessageID: The unique identifier of the article (mapped to the "_id" field in MongoDB).
// - Hash: of Message-ID (mapped to the "hash" field in MongoDB).
// - Newsgroups: A slice of strings containing the newsgroups associated with the article (mapped to the "newsgroups" field in MongoDB).
// - Head: A byte slice representing the head (header) of the article (mapped to the "head" field in MongoDB).
// - Headsize: An integer representing the size of the head in bytes (mapped to the "hs" field in MongoDB).
// - Body: A byte slice representing the body (content) of the article (mapped to the "body" field in MongoDB).
// - Bodysize: An integer representing the size of the body in bytes (mapped to the "bs" field in MongoDB).
// - Enc: An integer representing the encoding type of the article (mapped to the "enc" field in MongoDB).
// - Found: A boolean indicating whether the article was found during retrieval (not mapped to MongoDB).
type MongoArticle struct {
	MessageID     *string   `bson:"_id"`
	Hash          *string   `bson:"hash"`
	Newsgroups    []*string `bson:"newsgroups"`
	Head          *[]byte   `bson:"head"`
	Headsize      int       `bson:"hs"`
	Body          *[]byte   `bson:"body"`
	Bodysize      int       `bson:"bs"`
	Enc           int       `bson:"enc"`
	Found         bool
} // end type MongoArticle struct

/*
// MongoReqReturn represents the return value for a read request in MongoDB.
// It contains the following field:
// - Articles: A slice of pointers to MongoArticle objects representing the fetched articles.
type MongoReqReturn struct {
	Articles []*MongoArticle
} // end type MongoReqReturn struct
*/

// MongoGetRequest represents a read request for fetching articles from MongoDB.
// It contains the following fields:
//   - MessageIDs: A slice of messageIDes for which articles are requested.
//   - RetChan: A channel to receive the fetched articles as []*MongoArticle.
//   - STAT: Set to true to only CheckIfArticleExistsByMessageID
type MongoGetRequest struct {
	MessageIDs []*string
	STAT        bool
	RetChan     chan []*MongoArticle
} // end type MongoGetRequest struct

// MongoDelRequest represents a delete request for deleting articles from MongoDB.
// It contains the following fields:
// - MessageIDs: A slice of messageIDes for which articles are requested to be deleted.
// - RetChan: A channel to receive the count of deleted articles as int64.
type MongoDelRequest struct {
	MessageIDs []*string
	RetChan     chan int64
} // end type MongoDelRequest struct

// EOF mo_db_structs.go
