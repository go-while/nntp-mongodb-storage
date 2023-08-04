# nntp-mongodb-storage is a MongoDB Article Storage

This repository contains a Go module for storing Usenet articles in MongoDB.

It provides functionality for inserting and deleting articles asynchronously using worker goroutines, as well as support for compression using gzip and zlib.

## Getting Started

To use this module, you will need Go installed on your system. You can install the module using `go get`:

```shell
go get github.com/go-while/nntp-mongodb-storage
```

## Usage

See [https://github.com/go-while/nntp-mongodb_test](https://github.com/go-while/nntp-mongodb_test) for integration.
```shell
go get github.com/go-while/nntp-mongodb_test
```


# Load_MongoDB Function
The Load_MongoDB function is a part of the mongostorage package in the provided code.

It serves as a convenient initializer to set up the MongoDB configuration and
establishes connections to the MongoDB database and collections for article storage, deletion and read requests.

Function Signature
```go
func Load_MongoDB(mongoUri string, mongoDatabaseName string, mongoCollection string, mongoTimeout int64, delWorker int, delQueue int, insWorker int, insQueue int, getQueue int, getWorker int, testAfterInsert bool)
```

# Load_MongoDB: Usage

The `Load_MongoDB(...)` function accepts several parameters to control the behavior of the MongoDB storage backend and manage the worker goroutines.

This function is typically called at the start of the program to set up the MongoDB configuration and prepare the worker goroutines for article storage, deletion, and reading.

After calling this function, the main program can use the following channels to enqueue articles for deletion, insertion, and reads:

- `Mongo_Delete_queue`: This channel is used to enqueue articles for deletion, specifying their corresponding hashes, which will be processed by the delete worker goroutines.
- `Mongo_Insert_queue`: This channel is used to enqueue articles for insertion as `MongoArticle` instances, which will be processed by the insert worker goroutines. The `testAfterInsert` flag controls whether a test is performed after each article insertion, although specific details of this test are not provided in the code.
- `Mongo_Reader_queue`: This channel is used to enqueue read requests for articles, which will be processed by the reader worker goroutines. This allows concurrent reading of articles from the MongoDB collections.

These channels enable communication between the main program and the worker goroutines, allowing for concurrent handling of article deletions, insertions, and reading operations.

The integer parameters delWorker, insWorker, and getWorker determine the level of concurrency for article deletion, insertion, and read operations, respectively.

The integer parameters `delQueue`, `insQueue`, and `getQueue` control the length of the respective channels to manage buffering of requests before the send operation blocks.

# Load_MongoDB: Parameters
You can adjust the number of worker goroutines and queue sizes based on your application's requirements and the available resources.

- `mongoURI` (string): A string representing the MongoDB connection URI. It should include the necessary authentication details and the address of the MongoDB server.

- `mongoDatabaseName` (string): A string representing the name of the MongoDB database where the articles will be stored and retrieved from.

- `mongoCollection` (string): A string representing the name of the collection within the database where the articles will be stored.

- `mongoTimeout` (int64): A duration in milliseconds that defines the timeout for a connection attempt to the MongoDB server. If the connection is not successful within this duration, it will time out.

- `delWorker` (int): The number of worker goroutines to handle article deletions. It determines the level of concurrency for deletion operations. The delWorker parameter specifies the number of worker goroutines dedicated to handling article deletions. This parameter determines the level of concurrency for deletion operations. More delWorker values can improve the speed at which articles are deleted from the database.

- `delQueue` (int): The size of the delete queue. It specifies how many delete requests can be buffered before the send operation blocks. If delQueue is 0 or negative, a default value will be used. The delQueue parameter sets the maximum number of article deletion requests that can be buffered before the worker goroutines start processing them. When the number of articles to delete exceeds this limit, the send operation on the Mongo_Delete_queue channel will block until space becomes available in the queue.

- `insWorker` (int): The number of worker goroutines to handle article insertions. It determines the level of concurrency for insertion operations. The insWorker parameter determines the number of worker goroutines assigned to handle article insertions. This parameter controls the concurrency for insertion operations, allowing multiple articles to be inserted simultaneously.

- `insQueue` (int): The size of the insert queue. It specifies how many article insertion requests can be buffered before the send operation blocks. If insQueue is 0 or negative, a default value will be used. The insQueue parameter specifies the maximum number of article insertion requests that can be buffered in the queue before the worker goroutines process them. If the number of pending insertions exceeds this limit, the send operation on the Mongo_Insert_queue channel will block.

- `getWorker` (int): The number of worker goroutines to handle article reads. It determines the level of concurrency for read operations. The getWorker parameter sets the number of worker goroutines responsible for handling article reads. This parameter controls the level of concurrency for read operations, enabling multiple read requests to be processed concurrently.

- `getQueue` (int): The size of the read queue. It specifies how many read requests can be buffered before the send operation blocks. If getQueue is 0 or negative, a default value will be used. The getQueue parameter defines the maximum length of the read request queue. When the number of read requests exceeds this limit, the send operation on the Mongo_Reader_queue channel will block until space becomes available.

- `testAfterInsert` (bool): A flag indicating whether to perform a test after an article insertion. The specific test details are not provided in the function, and the flag can be used for application-specific testing purposes.


# MongoDB Storage Configuration

This document describes the default configuration options for the MongoDB storage package.

These constants define various settings used when no specific values are provided during the MongoDB setup.

## Default MongoDB Connection

- `DefaultMongoUri`: The default MongoDB connection string used when no URI is provided. It points to `mongodb://localhost:27017`, indicating the MongoDB server is running on the local machine on the default port 27017.

## Default Database and Collection Names

- `DefaultMongoDatabaseName`: The default name of the MongoDB database used when no database name is provided. It is set to "nntp" by default.

- `DefaultMongoCollection`: The default name of the MongoDB collection used when no collection name is provided. The collection name is set to "articles" by default.

## Default MongoDB Timeout

- `DefaultMongoTimeout`: The default timeout value (in seconds) for connecting to MongoDB. The timeout is set to 15 seconds.

## Default Worker and Queue Settings

- `DefaultDelWorker`: The number of `DeleteWorker` instances to start by default. It is set to 1.

- `DefaultDelQueue`: The number of delete requests allowed to be queued. It is set to 2.

- `DefaultDeleteBatchsize`: The number of Msgidhashes a `DeleteWorker` will cache before deleting to batch into one process. It is set to 1.

- `DefaultInsWorker`: The number of `InsertWorker` instances to start by default. It is set to 1.

- `DefaultInsQueue`: The number of insert requests allowed to be queued. It is set to 2.

- `DefaultInsertBatchsize`: The number of articles an `InsertWorker` will cache before inserting to batch into one process. It is set to 1.

- `DefaultGetWorker`: The number of `ReaderWorker` instances to start by default. It is set to 1.

- `DefaultGetQueue`: The total queue length for all `ReaderWorker` instances. It is set to 2.

Please note that these default values can be adjusted according to your specific requirements and available system resources.

Custom configurations can be provided when setting up the MongoDB storage package to optimize its performance for your application.


## Compression Constants (Magic Numbers)

- `NOCOMP` (int): Represents the value indicating no compression for articles. Value: 0

- `GZIP_enc` (int): Represents the value indicating GZIP compression for articles. Value: 1.

- `ZLIB_enc` (int): Represents the value indicating ZLIB compression for articles. Value: 2.


# MongoReadRequest Struct
```go
type MongoReadRequest struct {
	Msgidhashes []string
	RetChan     chan []*MongoArticle
} // end type MongoReadRequest struct
```

Let's explain the fields of the `MongoReadRequest` struct:

 - Msgidhashes: A slice of MessageIDHashes for which articles are requested. Each MessageIDHash uniquely identifies an article in the MongoDB collection.

 - RetChan: A channel used to receive the fetched articles as `[]*MongoArticle`.

 -   The fetched articles will be sent through this channel upon successful retrieval.

 - `[]*MongoArticle` is a slice of pointers to MongoArticle.
 - `[]*MongoArticle`: This is a slice (dynamic array) of pointers to MongoArticle objects. It can hold multiple pointers to MongoArticle objects, allowing for the representation of multiple articles.
 - `*MongoArticle`: This is a pointer to a MongoArticle object. Instead of holding the actual MongoArticle value, it holds the memory address where the MongoArticle is stored.

Using a slice of pointers is common when dealing with structures like databases or other data collections.
It allows for efficient memory usage and easy modification of elements in the collection without copying the entire content.
When fetching multiple articles from the MongoDB collection, it's common to use `[]*MongoArticle` to store and handle the results efficiently.


# MongoArticle Struct
MongoArticle: This is a struct type representing an article in the MongoDB collection.

It contains various fields like MessageIDHash, MessageID, Newsgroups, Head, Headsize, Body, Bodysize, Enc, and Found.

```go
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
```

The `MongoArticle` struct is a custom data type defined in the codebase, which represents an article to be stored in and retrieved from a MongoDB database.

It defines the structure and fields that will be used to map data between Go language objects and BSON (Binary JSON) documents in the MongoDB collection.

Let's explain the fields of the `MongoArticle` struct:

 - `MessageIDHash`: This field represents the unique identifier of the article and is mapped to the `_id` field in the MongoDB collection. The `_id` field in MongoDB is a special field that uniquely identifies each document in a collection.

 - `MessageID`: This field contains the message identifier of the article, which may be used for further identification and linking purposes. It is mapped to the `msgid` field in the MongoDB collection.

 - `Newsgroups`: This field is an array of strings that stores the newsgroups associated with the article. Newsgroups are used in Usenet systems to categorize and organize articles. In MongoDB, this field is mapped to the `newsgroups` array.

 - `Head`: This field represents the header of the article and is stored as a byte slice. The header typically contains metadata and additional information about the article. In MongoDB, the `head` field is mapped to a BSON binary field.

 - `Headsize`: This field stores the size of the header data in bytes. It is an integer field and is mapped to the `hs` field in the MongoDB collection.

 - `Body`: Similar to the `Head` field, this field represents the body of the article and is stored as a byte slice. The body contains the main content of the article. In MongoDB, the `body` field is mapped to a BSON binary field.

 - `Bodysize`: This field stores the size of the body data in bytes. It is an integer field and is mapped to the `bs` field in the MongoDB collection.

 - `Enc`: This field represents an integer indicating the type of encoding used for the article. The specific encoding types are not provided in the code snippet, but they could be custom encodings or standard ones like GZIP or ZLIB.

 - `Found`: This field is a boolean flag used during certain operations to indicate whether the article has been found or exists. It is not mapped to the MongoDB collection since it is not persisted but used internally within the application.

Overall, the `MongoArticle` struct defines the schema for articles to be stored in the MongoDB collection, and it provides a convenient way to interact with article data in the Go codebase while seamlessly mapping to and from MongoDB documents.


# Workers

- Multiple workers can be spawned concurrently to handle article inserts, deletes and reads in parallel.

- This concurrency can significantly improve the overall insertion performance, especially when dealing with a large number of articles.


# MongoInsertWorker

The MongoInsertWorker is designed to run as a goroutine, and to optimize the insert process, multiple workers can be spawned concurrently.

 - This concurrent approach enables parallel insertion of articles, leading to improved performance and reduced insert times, especially when dealing with a large number of articles.

 - The main program can control the level of concurrency by setting the number of MongoInsertWorker instances (insWorker) to be launched. Each worker will independently process insertion requests, making it possible to efficiently insert multiple articles simultaneously.

 - By utilizing multiple MongoInsertWorker goroutines, the system can take full advantage of available resources, effectively distributing the write workload across multiple threads, cores, or even machines if necessary. This concurrency model helps to avoid bottlenecks and ensures that write operations are executed efficiently, providing a responsive and performant writing experience for the application.

 - The MongoInsertWorker is a component of the mongostorage package responsible for handling the insertion of articles into a MongoDB database.

 - It operates as a worker goroutine, processing articles from a queue (Mongo_Insert_queue) and inserting them into the specified MongoDB collection.

 - Initialization: Before starting the insertion process, the MongoInsertWorker is created and initialized. It establishes a connection to the MongoDB database using the provided URI and database name.

 - Article Insertion: The worker listens to the Mongo_Insert_queue, which holds articles waiting to be inserted. As soon as an article becomes available in the queue, the worker dequeues it and starts the insertion process.

 - Duplicate Check: Before inserting the article, the worker performs a duplicate check based on the MessageIDHash of the article. It queries the MongoDB collection to check if an article with the same MessageIDHash already exists. This is to avoid inserting duplicate articles in the database.

 - Compression (Optional): Depending on the test case and configuration, the MongoInsertWorker may apply compression to the article's header and body before insertion. If compression is required, it compresses the data using either gzip or zlib compression algorithms.

 - Insertion: After the duplicate check and optional compression, the worker inserts the article into the specified MongoDB collection.

 - Logging: During the insertion process, the worker logs relevant information such as the raw size of the article, the size after compression (if applied), and the success or failure of the insertion.


# MongoDeleteWorker

Similar to the MongoInsertWorker, the MongoDeleteWorker is designed to operate as a goroutine with the potential for concurrent execution. By spawning multiple MongoDeleteWorker instances (delWorker), the system can take advantage of parallel processing to efficiently handle article deletions from the MongoDB database.

 - The concurrent nature of the MongoDeleteWorker allows for multiple worker goroutines to process article deletion requests concurrently. Each worker listens to the Mongo_Delete_queue, which holds article hashes waiting to be deleted. As soon as a hash becomes available in the queue, a worker dequeues it and initiates the deletion process.

 - With multiple MongoDeleteWorker instances running simultaneously, the application can efficiently handle a high volume of article deletion requests. This is particularly beneficial when dealing with large datasets or frequently changing data.

 - The MongoDeleteWorker is a component of the mongostorage package responsible for handling the deletion of articles from a MongoDB database.

 - It operates as a worker goroutine, processing article hashes from a queue (Mongo_Delete_queue) and deleting the corresponding articles from the specified MongoDB collection.

 - Initialization: Before starting the deletion process, the MongoDeleteWorker is created and initialized. It establishes a connection to the MongoDB database using the provided URI and database name.

 - Article Deletion: The worker listens to the Mongo_Delete_queue, which holds article hashes waiting to be deleted. As soon as a hash becomes available in the queue, the worker dequeues it and starts the deletion process.

 - Article Lookup: The worker uses the provided article hash to query the MongoDB collection and find the article with the corresponding MessageIDHash.

 - Deletion: Once the article is located based on the MessageIDHash, the worker proceeds to delete it from the MongoDB collection.

 - Logging: During the deletion process, the worker logs relevant information such as the article hash being processed and whether the deletion was successful or encountered an error.


# MongoReaderWorker

The MongoReaderWorker is designed to run as a goroutine, and to optimize the reading process, multiple workers can be spawned concurrently.

 - This concurrent approach enables parallel retrieval of articles, leading to improved performance and reduced read times, especially when dealing with a large number of articles.

 - The main program can control the level of concurrency by setting the number of MongoReaderWorker instances (getWorker) to be launched.

 - Each worker will independently process read requests, making it possible to efficiently retrieve multiple articles simultaneously.

 - By utilizing multiple MongoReaderWorker goroutines, the system can take full advantage of available resources, effectively distributing the read workload across multiple threads, cores, or even machines if necessary. This concurrency model helps to avoid bottlenecks and ensures that read operations are executed efficiently, providing a responsive and performant reading experience for the application.

- The MongoReaderWorker is a component of the mongostorage package responsible for handling read/get requests for articles to retrieve from a MongoDB database.

- It operates as a worker goroutine, processing read requests from a queue (Mongo_Reader_queue) and retrieving the corresponding articles from the specified MongoDB collection.

- The retrieved articles are then sent back to the main program through a return channel, allowing efficient and concurrent reading of articles from the database.

 - Initialization: Before starting the reading process, the MongoReaderWorker is created and initialized. It establishes a connection to the MongoDB database using the provided URI and database name.

 - Read Operations: The worker listens to the Mongo_Reader_queue, which holds read requests waiting to be processed. Each read request is represented as a `MongoReadRequest` struct containing a list of article hashes (`Msgidhashes`) and a return channel (`RetChan`) to send back the retrieved articles. As soon as a read request becomes available in the queue, the worker dequeues it and starts the reading process.

 - Article Retrieval: The worker uses the provided list of article hashes (`Msgidhashes`) to query the MongoDB collection and retrieve the corresponding articles. It searches for articles with matching `MessageIDHash` values in the collection.

 - Return Results: Once the articles are retrieved, the worker sends them back to the main program through the `RetChan` channel of the corresponding `MongoReadRequest` struct. This allows the main program to receive the requested articles and process them further.





# Contribution

Contributions to this project are welcome!

If you find any issues or have suggestions for improvements, feel free to open an issue or submit a pull request.


# Disclaimer

The code provided in this repository was mostly written by an AI language model (OpenAI's GPT-3.5, based on GPT-3) for demonstration purposes.

While efforts have been made to ensure its correctness, it is always recommended to review and validate the code before using it in production environments.


# License

This project is licensed under the MIT License - see the [LICENSE](https://choosealicense.com/licenses/mit/) file for details.

## Author
[go-while](https://github.com/go-while)
