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

# Load_MongoDB: Usage

The Load_MongoDB(cfg *MongoStorageConfig) function accepts a pointer to MongoStorageConfig as a parameter to control the behavior of the MongoDB storage backend and manage the worker goroutines.

- This function is typically called at the start of the program to set up the MongoDB configuration and prepare the worker goroutines.

- By passing the cfg pointer to Load_MongoDB, the function can directly modify the configuration values in the provided MongoStorageConfig object.

- After calling this function, the main program can use the following channels to enqueue articles for deletion, insertion, and read request:

- The use of channels (Mongo_Insert_queue, Mongo_Delete_queue, and Mongo_Reader_queue) helps coordinate and distribute the workload among these worker functions, enabling concurrent operations on the MongoDB collections.

- `Mongo_Delete_queue`: This channel is used to enqueue articles for deletion, specifying their corresponding hashes, which will be processed by the delete worker goroutines.
- `Mongo_Insert_queue`: This channel is used to enqueue articles for insertion as `MongoArticle` instances, which will be processed by the insert worker goroutines.
- `Mongo_Reader_queue`: This channel is used to enqueue read requests for articles, which will be processed by the reader worker goroutines.

- The integer parameters DelWorker, InsWorker, and GetWorker determine the level of concurrency for article deletion, insertion, and read operations, respectively.

- The integer parameters `DelQueue`, `InsQueue`, and `GetQueue` control the length of the respective channels to manage buffering of requests before the send operation blocks.

# Load_MongoDB: cfg Parameters
You can adjust the number of worker goroutines and queue sizes based on your application's requirements and the available resources.

- `MongoURI` (string): A string representing the MongoDB connection URI. It should include the necessary authentication details and the address of the MongoDB server.

- `MongoDatabaseName` (string): A string representing the name of the MongoDB database where the articles will be stored and retrieved from.

- `MongoCollection` (string): A string representing the name of the collection within the database where the articles will be stored.

- `MongoTimeout` (int64): A duration in milliseconds that defines the timeout for a connection attempt to the MongoDB server. If the connection is not successful within this duration, it will time out.

- `DelWorker` (int): The number of worker goroutines to handle article deletions. It determines the level of concurrency for deletion operations. The DelWorker parameter specifies the number of worker goroutines dedicated to handling article deletions. This parameter determines the level of concurrency for deletion operations. More DelWorker values can improve the speed at which articles are deleted from the database.

- `DelQueue` (int): The size of the delete queue. It specifies how many delete requests can be buffered before the send operation blocks. If DelQueue is 0 or negative, a default value will be used. The DelQueue parameter sets the maximum number of article deletion requests that can be buffered before the worker goroutines start processing them. When the number of articles to delete exceeds this limit, the send operation on the Mongo_Delete_queue channel will block until space becomes available in the queue.

- `DelBatch` (int): Sets the number of Msgidhashes a DeleteWorker will cache before deleting to batch into one process.

- `InsWorker` (int): The number of worker goroutines to handle article insertions. It determines the level of concurrency for insertion operations. The InsWorker parameter determines the number of worker goroutines assigned to handle article insertions. This parameter controls the concurrency for insertion operations, allowing multiple articles to be inserted simultaneously.

- `InsQueue` (int): The size of the insert queue. It specifies how many article insertion requests can be buffered before the send operation blocks. If InsQueue is 0 or negative, a default value will be used. The InsQueue parameter specifies the maximum number of article insertion requests that can be buffered in the queue before the worker goroutines process them. If the number of pending insertions exceeds this limit, the send operation on the Mongo_Insert_queue channel will block.

- `InsBatch` (int): Sets the number of Articles an InsertWorker will cache before inserting to batch into one process.

- `GetWorker` (int): The number of worker goroutines to handle article reads. It determines the level of concurrency for read operations. The GetWorker parameter sets the number of worker goroutines responsible for handling article reads. This parameter controls the level of concurrency for read operations, enabling multiple read requests to be processed concurrently.

- `GetQueue` (int): The size of the read queue. It specifies how many read requests can be buffered before the send operation blocks. If GetQueue is 0 or negative, a default value will be used. The GetQueue parameter defines the maximum length of the read request queue. When the number of read requests exceeds this limit, the send operation on the Mongo_Reader_queue channel will block until space becomes available.

- `TestAfterInsert` (bool): A flag indicating whether to perform a test after an article insertion. The specific test details are not provided in the function, and the flag can be used for application-specific testing purposes.


# MongoDB Storage Configuration

This document describes the default configuration options for the MongoDB storage package.

These constants define various settings used when no specific values are provided during the MongoDB setup.

## Default MongoDB Connection

- `DefaultMongoURI`: The default MongoDB connection string used when no URI is provided. It points to `mongodb://localhost:27017`, indicating the MongoDB server is running on the local machine on the default port 27017.

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


# MongoDB Workers

The MongoDB Workers are designed to handle article inserts, deletes, and reads concurrently, providing significant performance improvements when dealing with a large number of articles.

## MongoWorker_Insert

The MongoWorker_Insert runs as a goroutine and supports concurrent insertions of articles.

- MongoWorker_Insert is responsible for inserting articles into the specified MongoDB collection.

- By launching multiple MongoWorker_Insert instances concurrently (controlled by InsWorker), articles can be inserted in parallel, reducing insert times.

- This concurrent approach efficiently distributes the write workload across available resources, avoiding bottlenecks and ensuring efficient insertion of multiple articles simultaneously.

- Before starting the insertion process, the worker initializes and establishes a connection to the MongoDB database using the provided URI and database name.

- Upon receiving an article from the Mongo_Insert_queue, the worker performs a duplicate check based on the MessageIDHash to avoid inserting duplicates.

- Optionally, the worker can apply compression to the article's header and body before insertion, based on the test case and configuration.

- The worker then inserts the article into the MongoDB collection and logs relevant information such as raw size, compressed size (if applied), and the success or failure of the insertion.

## MongoWorker_Delete

The MongoWorker_Delete runs as a goroutine with concurrent execution capability.

- MongoWorker_Delete is responsible for deleting articles from the specified MongoDB collection.

- By using multiple MongoWorker_Delete instances concurrently (controlled by DelWorker), the system efficiently handles article deletions from the MongoDB database, particularly useful for large datasets or frequently changing data.

- The worker initializes and establishes a connection to the MongoDB database before starting the deletion process.

- Upon receiving an article hash from the Mongo_Delete_queue, the worker proceeds to delete it from the MongoDB collection (if found) and logs the relevant information.

## MongoWorker_Reader

The MongoWorker_Reader runs as a goroutine, supporting concurrent article retrieval.

- MongoWorker_Reader is responsible for handling read requests to retrieve articles from the MongoDB database.

- By launching multiple MongoWorker_Reader instances concurrently (controlled by GetWorker), articles can be retrieved in parallel, reducing read times.

- By using multiple MongoWorker_Reader instances concurrently (controlled by GetWorker), the system efficiently handles article deletions from the MongoDB database, particularly useful for large datasets or frequently changing data.

- Before starting the reading process, the worker initializes and establishes a connection to the MongoDB database.

- The worker listens to the Mongo_Reader_queue for read requests, each represented as a `MongoReadRequest` struct containing article hashes (`Msgidhashes`) and a return channel (`RetChan`) for sending back the retrieved articles.

- Upon receiving a read request, the worker queries the MongoDB collection to retrieve the corresponding articles based on the provided article hashes (`Msgidhashes`).

- Once the articles are retrieved, the worker sends them back to the main program through the `RetChan` channel of the corresponding `MongoReadRequest` struct, enabling efficient and concurrent reading of articles from the database.




# Contribution

Contributions to this project are welcome!

If you find any issues or have suggestions for improvements, feel free to open an issue or submit a pull request.


# Disclaimer

The code provided in this repository was partly written by an AI language model (OpenAI's GPT-3.5, based on GPT-3) for demonstration purposes.

While efforts have been made to ensure its correctness, it is always recommended to review and validate the code before using it in production environments.


# License

This project is licensed under the MIT License- see the [LICENSE](https://choosealicense.com/licenses/mit/) file for details.

## Author
[go-while](https://github.com/go-while)
