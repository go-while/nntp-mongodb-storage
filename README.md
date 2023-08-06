# nntp-mongodb-storage

## MongoDB Storage Package for Usenet Articles

- The mongostorage package offers a set of convenient functions for interacting with a MongoDB database and managing Usenet articles.

- It provides efficient and reliable methods for inserting, deleting, and retrieving Usenet articles in a MongoDB collection.

- With the mongostorage package, you can easily store, manage, and query Usenet articles in a MongoDB database, making it a valuable tool for any application that deals with Usenet data.

# Key Functions: mongostorage.Func(...)

- InsertOneArticle: Inserts a single article into the MongoDB collection.

- InsertManyArticles: Performs a bulk insert of multiple articles into the MongoDB collection.

- IsDup: Checks if an error is a duplicate key error in MongoDB.

- DeleteManyArticles: Deletes multiple articles from the MongoDB collection based on a list of MessageIDHashes.

- DeleteArticlesByMessageIDHash: Deletes an article from the MongoDB collection by its MessageIDHash.

- RetrieveArticleByMessageIDHash: Retrieves an article from the MongoDB collection by its MessageIDHash.

- RetrieveArticlesByMessageIDHashes: Retrieves articles from the MongoDB collection based on a list of MessageIDHashes.

- RetrieveHeadByMessageIDHash: Retrieves the "Head" data of an article based on its MessageIDHash.

- RetrieveBodyByMessageIDHash: Retrieves the "Body" data of an article based on its MessageIDHash.

- CheckIfArticleExistsByMessageIDHash: Checks if an article exists in the MongoDB collection based on its MessageIDHash.

# Using External Context

- The functions in the mongostorage package accept a `ctx context.Context` and mongo options, allowing you to provide your own MongoDB context from the main program.

- In Go, the `context.Context` is used to manage the lifecycle of operations and carry deadlines, cancellation signals, and other request-scoped values across API boundaries.

- It ensures the graceful termination of operations and handling of long-running tasks.

- To use these functions with an external context, you can create your MongoDB client and collection in the main program and pass the `context.Context` to the functions.

- This allows the MongoDB operations to be context-aware and respect the context's timeout and cancellation signals.

# Extending Context Timeout
- If you need to extend the timeout of the context for specific operations, you can use the ExtendContextTimeout function provided by the package.

- Here's an example of how to use it:
```go
// Extending the context timeout for a specific operation
newCtx, newCancel := mongostorage.ExtendContextTimeout(ctx, cancel, 20)
// defer newCancel() <- defer is only needed if you dont call ExtendContextTimeout again. pass `cancel` to ExtendContextTimeout to cancel there.
// Now use the newCtx for the MongoDB operation
```

- Here's an example of how you can use the mongostorage functions with an external context:

```go
package main

import (
	"context"
	"log"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"yourproject/mongostorage"
)

func main() {
	// Create a MongoDB client with a timeout for connecting to the server
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI("mongodb://localhost:27017").SetConnectTimeout(5*time.Second))
	if err != nil {
		log.Fatalf("Error connecting to MongoDB: %v", err)
	}
	defer client.Disconnect(context.Background())

	// Get a reference to the MongoDB collection
	collection := client.Database("yourdbname").Collection("yourcollectionname")

	// Create a context with a timeout and pass it to the functions in the mongostorage package
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Example of inserting a single article
	article := &mongostorage.MongoArticle{
		// Populate your article data here
	}
	err = mongostorage.InsertOneArticle(ctx, collection, article)
	if err != nil {
		log.Printf("Error inserting article: %v", err)
	}

	// Example of inserting multiple articles in bulk
	articles := []*mongostorage.MongoArticle{
		{
			MessageIDHash: strPtr("hash1"),
			MessageID:     strPtr("msgid1"),
			Newsgroups:    []string{"group1", "group2"},
			Head:          []byte("This is the head of article 1"),
			Headsize:      100,
			Body:          []byte("This is the body of article 1"),
			Bodysize:      200,
			Enc:           0, // not compressed
		},
		{
			MessageIDHash: strPtr("hash1"),
			MessageID:     strPtr("msgid1"),
			Newsgroups:    []string{"group1", "group2"},
			Head:          []byte("This is the head of article 1"),
			Headsize:      100,
			Body:          []byte("This is the body of article 1"),
			Bodysize:      200,
			Enc:           1, // indicator: compressed with GZIP (sender has to apply de/compression)
		},
		{
			MessageIDHash: strPtr("hash2"),
			MessageID:     strPtr("msgid2"),
			Newsgroups:    []string{"group3", "group4"},
			Head:          []byte("This is the head of article 2"),
			Headsize:      150,
			Body:          []byte("This is the body of article 2"),
			Bodysize:      300,
			Enc:           2, // indicator: compressed with ZLIB (sender has to apply de/compression)
		},
		// Add more articles as needed
	}

	err = mongostorage.InsertManyArticles(ctx, collection, articles)
	if err != nil {
		log.Printf("Error inserting articles: %v", err)
	} else {
		log.Println("Articles inserted successfully.")
	}

	// Check if the error is a duplicate key error using the IsDup function.
	if IsDup(err) {
		log.Println("Duplicate key error: The document already exists.")
		// Handle the duplicate key error here, if needed.
	} else {
		log.Println("Other error occurred:", err)
		// Handle other write errors, if needed.
	}

	// Example of retrieving an article
	messageIDHash := "your-article-message-id-hash"
	article, err := mongostorage.RetrieveArticleByMessageIDHash(ctx, collection, messageIDHash)
	if err != nil {
		log.Printf("Error retrieving article: %v", err)
	} else if article != nil {
		log.Println("Article found:", article)
	} else {
		log.Println("Article not found.")
	}

	// Example of retrieving multiple articles based on a list of MessageIDHashes
	msgIDHashes := []*string{"hash1", "hash2", "hash3"}
	articles, err := mongostorage.RetrieveArticleByMessageIDHashes(ctx, collection, msgIDHashes)
	if err != nil {
		log.Printf("Error retrieving articles: %v", err)
	} else {
		for _, article := range articles {
			if article != nil {
				log.Println("Article found:", article)
			} else {
				log.Println("Article not found.")
			}
		}
	}

	// Example of deleting article(s).
	msgidhashes := []string{"hash1", "hash2", "hash3"}
	success := mongostorage.DeleteManyArticles(ctx, collection, msgidhashes)
	if success {
		log.Println("Articles deleted successfully.")
	}

}
```

- The `MessageIDHash` field in the `MongoArticle` struct serves as an identifier for each article in the MongoDB collection.

- The name "hash" in `MessageIDHash` does not necessarily imply that it must be a cryptographic hash; it can be any string, and it acts as a unique identifier for the document in the collection.

- The field `MessageIDHash` is connected to the MongoDB's special `_id` field, which uniquely identifies each document in a collection.

- MongoDB requires each document to have a unique value for its `_id` field.

- By setting the MessageIDHash as the value of the `_id` field, we ensure that each article is uniquely identified in the collection.

- Additionally, the MongoArticle struct also contains a separate field named MessageID, which can store the original, non-hashed version of the article's identifier.

- This allows developers to store both the original identifier and the hashed identifier in the same document, providing flexibility and convenience when working with the articles.


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

- After calling this function, the main program can use the following channels to enqueue articles for deletion, insertion, and read request:

- The use of channels (Mongo_Insert_queue, Mongo_Delete_queue, and Mongo_Reader_queue) helps coordinate and distribute the workload among these worker functions, enabling concurrent operations on the MongoDB collections.

- `Mongo_Delete_queue`: This channel is used to enqueue articles for deletion, specifying their corresponding hashes, which will be processed by the delete worker goroutines.
- `Mongo_Insert_queue`: This channel is used to enqueue articles for insertion as `MongoArticle` instances, which will be processed by the insert worker goroutines.
- `Mongo_Reader_queue`: This channel is used to enqueue read requests for articles, which will be processed by the reader worker goroutines.

- The integer parameters `DelWorker`, `InsWorker`, and `GetWorker` determine the level of concurrency for article deletion, insertion, and read operations, respectively.

- The integer parameters `DelQueue`, `InsQueue`, and `GetQueue` control the length of the respective channels to manage buffering of requests before the send operation blocks.

# Load_MongoDB: cfg Parameters
- You can adjust the number of worker goroutines and queue sizes based on your application's requirements and the available resources.

- Custom configurations can be provided when setting up the MongoDB storage package to optimize its performance for your application.

- `MongoURI` (string): A string representing the MongoDB connection URI. It should include the necessary authentication details and the address of the MongoDB server.

- `MongoDatabaseName` (string): A string representing the name of the MongoDB database where the articles will be stored and retrieved from.

- `MongoCollection` (string): A string representing the name of the collection within the database where the articles will be stored.

- `MongoTimeout` (int64): A duration in milliseconds that defines the timeout for a connection attempt to the MongoDB server. If the connection is not successful within this duration, it will time out.

- `DelWorker` (int): The number of worker goroutines to handle article deletions. It determines the level of concurrency for deletion operations. The DelWorker parameter specifies the number of worker goroutines dedicated to handling article deletions. This parameter determines the level of concurrency for deletion operations. More DelWorker values can improve the speed at which articles are deleted from the database.

- `DelQueue` (int): The size of the delete queue. It specifies how many delete requests can be buffered before the send operation blocks. If DelQueue is 0 or negative, a default value will be used. The DelQueue parameter sets the maximum number of article deletion requests that can be buffered before the worker goroutines start processing them. When the number of articles to delete exceeds this limit, the send operation on the Mongo_Delete_queue channel will block until space becomes available in the queue.

- `DelBatch` (int): The number of Msgidhashes one DeleteWorker will cache before processing deletions in a batch operation.

- `InsWorker` (int): The number of worker goroutines to handle article insertions. It determines the level of concurrency for insertion operations. The InsWorker parameter determines the number of worker goroutines assigned to handle article insertions. This parameter controls the concurrency for insertion operations, allowing multiple articles to be inserted simultaneously.

- `InsQueue` (int): The size of the insert queue. It specifies how many article insertion requests can be buffered before the send operation blocks. If InsQueue is 0 or negative, a default value will be used. The InsQueue parameter specifies the maximum number of article insertion requests that can be buffered in the queue before the worker goroutines process them. If the number of pending insertions exceeds this limit, the send operation on the Mongo_Insert_queue channel will block.

- `InsBatch` (int): The number of articles one InsertWorker will cache before inserting them in a batch operation.

- `GetWorker` (int): The number of worker goroutines to handle article reads. It determines the level of concurrency for read operations. The GetWorker parameter sets the number of worker goroutines responsible for handling article reads. This parameter controls the level of concurrency for read operations, enabling multiple read requests to be processed concurrently.

- `GetQueue` (int): The size of the read queue. It specifies how many read requests can be buffered before the send operation blocks. If GetQueue is 0 or negative, a default value will be used. The GetQueue parameter defines the maximum length of the read request queue. When the number of read requests exceeds this limit, the send operation on the Mongo_Reader_queue channel will block until space becomes available.

- `FlushTimer`: The time in milliseconds for flushing batched operations to MongoDB. Default = 1000

- Setting `FlushTimer` too low may waste CPU cycles, and if you want instant processing, keep the Insert/DeleteBatchsizes as 1 (default).

- `TestAfterInsert` (bool): A flag indicating whether to perform a test after an article insertion. The specific test details are not provided in the function, and the flag can be used for application-specific testing purposes.



## Compression Constants (Magic Numbers)

- `NOCOMP` (int): Represents the value indicating no compression for articles. Value: 0

- `GZIP_enc` (int): Represents the value indicating GZIP compression for articles. Value: 1

- `ZLIB_enc` (int): Represents the value indicating ZLIB compression for articles. Value: 2


# MongoReadRequest Struct
```go
type MongoReadRequest struct {
	Msgidhashes []string
	RetChan     chan []*MongoArticle
} // end type MongoReadRequest struct
```

#Explanation of the fields in the MongoReadRequest struct:

- Msgidhashes: A slice of MessageIDHashes for which articles are requested. Each MessageIDHash uniquely identifies an article in the MongoDB collection. This field allows the mongoWorker_Reader to know which articles to retrieve from the database.

- RetChan: A channel used to receive the fetched articles as []*MongoArticle. The fetched articles will be sent through this channel upon successful retrieval.

- []*MongoArticle: This is a slice (dynamic array) of pointers to MongoArticle objects. It can hold multiple pointers to MongoArticle objects, allowing for the representation of multiple articles that are fetched from the database.

- *MongoArticle: This is a pointer to a MongoArticle object. Instead of holding the actual MongoArticle value, it holds the memory address where the MongoArticle is stored. Using pointers allows for efficient memory usage when dealing with large datasets, as only the memory addresses are passed around, rather than duplicating the entire data.

- *MongoArticle.Found: This is a boolean flag that indicates whether the requested article with the corresponding MessageIDHash was found in the MongoDB collection or not. When the mongoWorker_Reader retrieves an article, it sets this flag to 'true'. If the article is not found in the database, the flag is set to 'false'.




# MongoArticle Struct
The MongoArticle struct represents an article in the MongoDB collection.

It contains various fields to store information about the article.

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

- MessageIDHash: The unique identifier (hash) for the article, represented as a string and mapped to the `_id` field in the MongoDB collection.

- `MessageID`: The Message-ID of the article, represented as a string and mapped to the `msgid` field in the MongoDB collection.

- `Newsgroups`: A slice of strings representing the newsgroups associated with the article. It is mapped to the `newsgroups` field in the MongoDB collection.

- `Head`: The header of the article, represented as a byte slice and mapped to the `head` field in the MongoDB collection.

- `Headsize`: The size of the article's header in bytes, represented as an integer and mapped to the `hs` field in the MongoDB collection.

- `Body`: The body of the article, represented as a byte slice and mapped to the `body` field in the MongoDB collection.

- `Bodysize`: The size of the article's body in bytes, represented as an integer and mapped to the `bs` field in the MongoDB collection.

- `Enc`: An integer representing the encoding type of the article, mapped to the `enc` field in the MongoDB collection.

- `Found`: A boolean flag that is used to indicate whether the article was found during a retrieval operation. It is initially set to false and may be modified by the mongoWorker_Reader to indicate if an article was successfully retrieved.



## Workers

Multiple workers can be spawned concurrently to handle article inserts, deletes, and reads in parallel.

This concurrency significantly improves the overall insertion performance, especially when dealing with a large number of articles.

## mongoWorker_Insert

mongoWorker_Insert is responsible for inserting articles into the specified MongoDB collection.

- By launching multiple mongoWorker_Insert instances concurrently (controlled by InsWorker), articles can be inserted in parallel, reducing insert times.
- This concurrent approach efficiently distributes the write workload across available resources, avoiding bottlenecks and ensuring efficient insertion of multiple articles simultaneously.
- Before starting the insertion process, the worker initializes and establishes a connection to the MongoDB database using the provided URI and database name.
- Upon receiving an article from the Mongo_Insert_queue, the worker performs a duplicate check based on the MessageIDHash to avoid inserting duplicates.
- When using compression, it is advisable to set the Enc field of the Articles struct to the corresponding value (e.g., mongostorage.GZIP_enc or mongostorage.ZLIB_enc) so that it can be identified and decompressed correctly later during retrieval.
- The worker then inserts the article into the MongoDB collection and logs relevant information such as raw size, compressed size (if applied), and the success or failure of the insertion.


## mongoWorker_Delete

mongoWorker_Delete is responsible for deleting articles from the specified MongoDB collection.

- By using multiple mongoWorker_Delete instances concurrently (controlled by DelWorker), the system efficiently handles article deletions from the MongoDB database, particularly useful for large datasets or frequently changing data.
- The worker initializes and establishes a connection to the MongoDB database before starting the deletion process.
- Upon receiving an article hash from the Mongo_Delete_queue, the worker proceeds to delete it from the MongoDB collection and logs the relevant information.


## mongoWorker_Reader

mongoWorker_Reader is responsible for handling read requests to retrieve articles from the MongoDB database.

- By launching multiple mongoWorker_Reader instances concurrently (controlled by GetWorker), articles can be retrieved in parallel, reducing read times.
- Before starting the reading process, the worker initializes and establishes a connection to the MongoDB database.
- The worker listens to the Mongo_Reader_queue for read requests, each represented as a `MongoReadRequest` struct containing article hashes (`Msgidhashes`) and a return channel (`RetChan`) for sending back the retrieved articles.
- Upon receiving a read request, the worker queries the MongoDB collection to retrieve the corresponding articles (in compressed form) based on the provided article hashes (`Msgidhashes`).
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
