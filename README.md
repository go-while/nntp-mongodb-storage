# nntp-mongodb-storage

## MongoDB Storage Package for Usenet Articles

- The mongostorage package offers a set of convenient functions for interacting with a MongoDB database and managing Usenet articles or anything else if your needs are satisfied by `type MongoArticle struct`.

- It provides efficient and reliable methods for inserting, deleting, and retrieving Usenet articles in a MongoDB collection.

- With the mongostorage package, you can easily store, manage, and query Usenet articles in a MongoDB database, making it a valuable tool for any application that deals with Usenet data.

# Key Functions: mongostorage.Func(...)

- InsertOneArticle: Inserts a single article into the MongoDB collection.

- InsertManyArticles: Performs a bulk insert of multiple articles into the MongoDB collection.

- IsDup: Checks if an error is a duplicate key error in MongoDB.

- DeleteManyArticles: Deletes multiple articles from the MongoDB collection based on a list of MessageIDes.

- DeleteArticlesByMessageID: Deletes an article from the MongoDB collection by its MessageID.

- RetrieveArticleByMessageID: Retrieves an article from the MongoDB collection by its MessageID.

- RetrieveArticlesByMessageIDs: Retrieves articles from the MongoDB collection based on a list of MessageIDes.

- RetrieveHeadByMessageID: Retrieves the "Head" data of an article based on its MessageID.

- RetrieveBodyByMessageID: Retrieves the "Body" data of an article based on its MessageID.

- CheckIfArticleExistsByMessageID: Checks if an article exists in the MongoDB collection based on its MessageID.

# Using External Context

- The functions in the mongostorage package accept a `ctx context.Context` and mongo options, allowing you to provide your own MongoDB context from the main program.

- In Go, the `context.Context` is used to manage the lifecycle of operations and carry deadlines, cancellation signals, and other request-scoped values across API boundaries.

- It ensures the graceful termination of operations and handling of long-running tasks.

- To use these functions with an external context, you can create your MongoDB client and collection in the main program and pass the `context.Context` to the functions.

- This allows the MongoDB operations to be context-aware and respect the context's timeout and cancellation signals.

# MongoWorker_UpDn_Scaler(cfg)

The purpose of `MongoWorker_UpDn_Scaler(cfg)` is to provide a mechanism for dynamically adjusting the number of worker goroutines in response to changes in workload or other factors.

It listens on specific channels (`mongostorage.UpDn_*_Worker_chan`) to receive requests (bool: [`true`|`false`] for starting or stopping specific types of worker goroutines.

### Functionality

- **Listening for Up/Down Requests**: `MongoWorker_UpDn_Scaler(cfg)` continuously listens on channels (`UpDn_Reader_Worker_chan`, `UpDn_Delete_Worker_chan`, `UpDn_Insert_Worker_chan`) to receive requests for scaling up or down specific types of worker goroutines.

- **Background Processing**: `MongoWorker_UpDn_Scaler(cfg)` is designed to run in the background continuously, processing up and down requests as they arrive. It remains active throughout the lifetime of the program, ensuring that the number of worker goroutines can be flexibly adjusted without interrupting the main program's flow.

- **Starting or Stopping Workers**: Based on the requests, the `MongoWorker_UpDn_Scaler(cfg)` function initiates the start or stop of the respective worker goroutines.

## External Usage of `MongoWorker_UpDn_Scaler(cfg)`

The `MongoWorker_UpDn_Scaler(cfg)` function plays a crucial role in dynamically managing the scaling of worker goroutines in the MongoDB storage package. It allows external components to control the number of worker goroutines based on up and down requests, thus optimizing resource usage and performance. Here's an explanation of how this function can be used externally:

1. **Starting `MongoWorker_UpDn_Scaler(cfg)`**: The `mongostroage.UpDn_*_Worker_chan` channels are already initialized upon importing the module, you can directly start `MongoWorker_UpDn_Scaler(cfg)` by calling the function and passing it a `MongoStorageConfig` object as an argument. This config object should contain the initial worker counts for each type of worker (e.g., `GetWorker`, `DelWorker`, `InsWorker`).

```go
	// launch to background
	go mongostorage.MongoWorker_UpDn_Scaler(cfg)
```

2. **Control Worker Scaling**: After `MongoWorker_UpDn_Scaler(cfg)` is running in the background, you can control the scaling of worker goroutines by sending `true` or `false` signals to the respective worker channels.

- To increase the number of worker goroutines of a specific type (e.g., reader, delete, or insert), send a `true` signal to the corresponding worker channel (e.g., `UpDn_Reader_Worker_chan`, `UpDn_Delete_Worker_chan`, or `UpDn_Insert_Worker_chan`).

- To decrease the number of worker goroutines of a specific type, send a `false` signal to the respective worker channel.

3. **Stopping All Workers**: If you want to stop all worker goroutines simultaneously, you can send a `true` signal to the `UpDn_StopAll_Worker_chan`. This will trigger signals to all worker channels, effectively instructing all workers to stop gracefully.

Overall, by using `MongoWorker_UpDn_Scaler(cfg)` and the pre-initialized worker channels, the external application can dynamically adjust the number of worker goroutines based on workload demands or other factors, enabling efficient utilization of resources and improved performance for MongoDB storage operations.


# Extending Context Timeout
- If you need to extend the timeout of the context for specific operations, you can use the ExtendContextTimeout function provided by the package.

- Here's an example of how to use it:
```go
// Extending the context timeout for a specific operation
newCtx, newCancel := mongostorage.ExtendContextTimeout(ctx, cancel, 20)
// defer newCancel() <- defer is only needed if you dont call ExtendContextTimeout again. pass `cancel` to ExtendContextTimeout to cancel there.
// Now use the newCtx for the MongoDB operation
```

Use the mongostorage functions with an external context:

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

	messageID1, messageID2, messageID3 := "<messageID1@local.test>", "<messageID2@local.test>", "<messageID3@local.test>"
	head1 := []byte("This is the head of article 1")
	body1 := []byte("This is the body of article 1")
	head2 := []byte("This is the head of article 2")
	body2 := []byte("This is the body of article 2")
	head3 := []byte("This is the head of article 3")
	body3 := []byte("This is the body of article 3")

	// Example of inserting a single article
	article := &mongostorage.MongoArticle{
			MessageID:     messageID1,
			Newsgroups:    []string{"group1", "group2"},
			Head:          head1,
			Headsize:      len(head1),
			Body:          body1,
			Bodysize:      len(body1),
			Enc:           0, // not compressed
	}
	err = mongostorage.InsertOneArticle(ctx, collection, article)
	if err != nil {
		log.Printf("Error inserting article: %v", err)
	}

	// Example of inserting multiple articles in bulk
	articles := []*mongostorage.MongoArticle{
		{
			MessageID:     messageID1,
			Newsgroups:    []string{"group1", "group2"},
			Head:          head1,
			Headsize:      len(head1),
			Body:          body1,
			Bodysize:      len(body1),
			Enc:           0, // not compressed
		},
		{
			MessageID:     messageID1,  // will generate a duplicate error
			Newsgroups:    []string{"group1", "group2"},
			Head:          head1,
			Headsize:      len(head1),
			Body:          body1,
			Bodysize:      len(body1),
			Enc:           1, // indicator: compressed with GZIP (sender has to apply de/compression)
		},
		{
			MessageID:     messageID2,
			Newsgroups:    []string{"group3", "group4"},
			Head:          head2,
			Headsize:      len(head2),
			Body:          body2,
			Bodysize:      len(body2),
			Enc:           2, // indicator: compressed with ZLIB (sender has to apply de/compression)
		},
		{
			MessageID:     messageID,
			Newsgroups:    []string{"group3", "group4"},
			Head:          head3,
			Headsize:      len(head3),
			Body:          body3,
			Bodysize:      len(body3),
			Enc:           3, // indicator: compressed with FLATE (sender has to apply de/compression)
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
	messageID := "<test@message-id.test>"
	article, err := mongostorage.RetrieveArticleByMessageID(ctx, collection, messageID)
	if err != nil {
		log.Printf("Error retrieving article: %v", err)
	} else if article != nil {
		log.Println("Article found:", article)
	} else {
		log.Println("Article not found.")
	}

	// Example of retrieving multiple articles based on a list of MessageIDes
	messageIDs := []string{ messageID1, messageID2, messageID3, ...}
	articles, err := mongostorage.RetrieveArticleByMessageIDes(ctx, collection, messageIDs)
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
	messageIDs := []string{messageID, messageID2, messageID3}
	success := mongostorage.DeleteManyArticles(ctx, collection, messageIDs)
	if success {
		log.Println("Articles deleted successfully.")
	}

}
```

- The field `MessageID` is connected to the MongoDB's special `_id` field, which uniquely identifies each document in a collection.

- MongoDB requires each document to have a unique value for its `_id` field.

- By setting the MessageID as the value of the `_id` field, we ensure that each article is uniquely identified in the collection.

- Additionally, the MongoArticle struct also contains a separate field named `Hash`, which can store the hashed version of the article's identifier.


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

- `Mongo_Delete_queue`: This channel is used to enqueue articles for deletion, specifying their corresponding Message-ID, which will be processed by the delete worker goroutines.
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

- `DelBatch` (int): The number of MessageIDs one DeleteWorker will cache before processing deletions in a batch operation.

- `InsWorker` (int): The number of worker goroutines to handle article insertions. It determines the level of concurrency for insertion operations. The InsWorker parameter determines the number of worker goroutines assigned to handle article insertions. This parameter controls the concurrency for insertion operations, allowing multiple articles to be inserted simultaneously.

- `InsQueue` (int): The size of the insert queue. It specifies how many article insertion requests can be buffered before the send operation blocks. If InsQueue is 0 or negative, a default value will be used. The InsQueue parameter specifies the maximum number of article insertion requests that can be buffered in the queue before the worker goroutines process them. If the number of pending insertions exceeds this limit, the send operation on the Mongo_Insert_queue channel will block.

- `InsBatch` (int): The number of articles one InsertWorker will cache before inserting them in a batch operation.

- `GetWorker` (int): The number of worker goroutines to handle article reads. It determines the level of concurrency for read operations. The GetWorker parameter sets the number of worker goroutines responsible for handling article reads. This parameter controls the level of concurrency for read operations, enabling multiple read requests to be processed concurrently.

- `GetQueue` (int): The size of the read queue. It specifies how many read requests can be buffered before the send operation blocks. If GetQueue is 0 or negative, a default value will be used. The GetQueue parameter defines the maximum length of the read request queue. When the number of read requests exceeds this limit, the send operation on the Mongo_Reader_queue channel will block until space becomes available.

- `FlushTimer`: The time in milliseconds for flushing batched operations to MongoDB. Default = 1000

- Setting `FlushTimer` too low may waste CPU cycles, and if you want instant processing, keep the Insert/DeleteBatchsizes as 1 (default).

- `TestAfterInsert` (bool): A flag indicating whether to perform a test after an article insertion. The specific test details are not provided in the function, and the flag can be used for application-specific testing purposes.


## Compression Constants (Magic Numbers)

These Compression Constants are indicators so you know which algo is needed for decompression.

Compression has to be applied by sender before inserting the articles.


- `NOCOMP` (int): Represents the value indicating no compression for article. Value: 0 (default)

- `GZIP_enc` (int): Represents the value indicating GZIP compression for article. Value: 1

- `ZLIB_enc` (int): Represents the value indicating ZLIB compression for article. Value: 2

- `FLATE_enc` (int): Represents the value indicating FLATE compression for article. Value: 3



# MongoGetRequest
```go
// MongoGetRequest represents a read request for fetching articles from MongoDB.
// It contains the following fields:
//   - MessageIDs: A slice of messageIDes for which articles are requested.
//   - RetChan: A channel to receive the fetched articles as []*MongoArticle.
//   - STAT: Set to true to only CheckIfArticleExistsByMessageID
type MongoGetRequest struct {
	MessageIDs []string
	STAT        bool
	RetChan     chan []*MongoArticle
} // end type MongoGetRequest struct
```

# MongoDelRequest
```go
// MongoDelRequest represents a delete request for deleting articles from MongoDB.
// It contains the following fields:
// - MessageIDs: A slice of messageIDes for which articles are requested to be deleted.
// - RetChan: A channel to receive the count of deleted articles as int64.
type MongoDelRequest struct {
	MessageIDs []string
	RetChan     chan int64
} // end type MongoDelRequest struct
```

- `[]MessageIDs`: A slice of strings, representing a list of MessageIDes for which articles are requested. Each MessageID uniquely identifies an article in the MongoDB collection.

- `[]*MongoArticle`: This is a slice of pointers to `MongoArticle` objects. It can hold multiple pointers to `MongoArticle` objects, allowing for the representation of multiple articles that are fetched from the database.


# MongoArticle Struct
The MongoArticle struct represents an article in the MongoDB collection.

It contains various fields to store information about the article.

```go
type MongoArticle struct {
	MessageID     string   `bson:"_id"`
	Hash          string   `bson:"hash"`
	Newsgroups    []string `bson:"ng"`
	Head          []byte   `bson:"head"`
	Headsize      int       `bson:"hs"`
	Body          []byte   `bson:"body"`
	Bodysize      int       `bson:"bs"`
	Arrival       int64     `bson:"at"`
	HeadDate      int64     `bson:"hd"`
	Enc           int       `bson:"enc"`
	Found         bool
} // end type MongoArticle struct
```
The `MongoArticle` struct is a custom data type defined in the codebase, which represents an article to be stored in and retrieved from a MongoDB database.

- `MessageID`: The unique identifier for the article, stored as a pointer to a string and mapped to the `_id` field in the MongoDB collection.

- `Newsgroups`: This field is a slice of pointers to strings, representing the newsgroups linked to the article. It is mapped to the `newsgroups` field in the MongoDB collection.

- `Head`: The header of the article, stored as a pointer to a byte slice and mapped to the `head` field in the MongoDB collection.

- `Headsize`: The size of the article's header in bytes as an integer and mapped to the `hs` field in the MongoDB collection.

- `Body`: The body of the article, stored as a pointer to a byte slice and mapped to the `body` field in the MongoDB collection.

- `Bodysize`: The size of the article's body in bytes, represented as an integer and mapped to the `bs` field in the MongoDB collection.

- `Arrival`: An integer64 (unixtime) representing the arrival time(stamp) of the article, mapped to the `at` field in the MongoDB collection.

- `HeadDate`: An integer64 (unixtime) representing the header time(stamp) of the article, mapped to the `hd` field in the MongoDB collection.

- `Enc`: An integer representing the encoding/compression type of the article, mapped to the `enc` field in the MongoDB collection.

- `Found`: A boolean flag that is used to indicate whether the article was found during a retrieval operation. It is initially set to `false` and may be modified by the mongoWorker_Reader to indicate if an article was successfully retrieved.


## Workers

Multiple workers can be spawned concurrently to handle article inserts, deletes, and reads in parallel.

This concurrency significantly improves the overall insertion performance, especially when dealing with a large number of articles.

## mongoWorker_Insert

mongoWorker_Insert is responsible for inserting articles into the specified MongoDB collection.

- By launching multiple mongoWorker_Insert instances concurrently (controlled by InsWorker), articles can be inserted in parallel, reducing insert times.
- This concurrent approach efficiently distributes the write workload across available resources, avoiding bottlenecks and ensuring efficient insertion of multiple articles simultaneously.
- Before starting the insertion process, the worker initializes and establishes a connection to the MongoDB database using the provided URI and database name.
- Upon receiving an article from the Mongo_Insert_queue, the worker performs a duplicate check based on the MessageID to avoid inserting duplicates.
- When using compression, it is advisable to set the Enc field of the Articles struct to the corresponding value (e.g., mongostorage.GZIP_enc or mongostorage.ZLIB_enc) so that it can be identified and decompressed correctly later during retrieval.
- The worker then inserts the article into the MongoDB collection and logs relevant information such as raw size, compressed size (if applied), and the success or failure of the insertion.


## mongoWorker_Delete

mongoWorker_Delete is responsible for deleting articles from the specified MongoDB collection.

- By using multiple mongoWorker_Delete instances concurrently (controlled by DelWorker), the system efficiently handles article deletions from the MongoDB database, particularly useful for large datasets or frequently changing data.
- The worker initializes and establishes a connection to the MongoDB database before starting the deletion process.
- Upon receiving an messageID from the Mongo_Delete_queue, the worker proceeds to delete it from the MongoDB collection and logs the relevant information.


## mongoWorker_Reader

mongoWorker_Reader is responsible for handling read requests to retrieve articles from the MongoDB database.

- By launching multiple mongoWorker_Reader instances concurrently (controlled by GetWorker), articles can be retrieved in parallel, reducing read times.
- Before starting the reading process, the worker initializes and establishes a connection to the MongoDB database.
- The worker listens to the Mongo_Reader_queue for read requests, each represented as a `MongoGetRequest` struct containing article (`MessageIDs`) and a return channel (`RetChan`) for sending back the retrieved articles.
- Upon receiving a read request, the worker queries the MongoDB collection to retrieve the corresponding articles (in compressed form) based on the provided article (`MessageIDs`).
- Once the articles are retrieved, the worker sends them back to the main program through the `RetChan` channel of the corresponding `MongoGetRequest` struct, enabling efficient and concurrent reading of articles from the database.


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
