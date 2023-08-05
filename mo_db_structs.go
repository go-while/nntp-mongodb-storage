package mongostorage

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
const NOCOMP int = 0

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
	MessageIDHash *string  `bson:"_id"`
	MessageID     *string  `bson:"msgid"`
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
