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
The Load_MongoDB function is a part of the mongostorage package in the provided code. It serves as a convenient initializer to set up the MongoDB configuration and establish connections to the MongoDB database and collections for article storage and deletion.

Function Signature
```go
func Load_MongoDB(mongoUri string, mongoDatabaseName string, mongoCollection string, mongoTimeout int64, delWorker int, delQueue int, insWorker int, insQueue int, getQueue int, getWorker int, testAfterInsert bool)
```

# Load_MongoDB Parameters
```
mongoURI (string): A string representing the MongoDB connection URI. It should include the necessary authentication details and the address of the MongoDB server.
mongoDatabaseName (string): A string representing the name of the MongoDB database where the articles will be stored and retrieved from.
mongoCollection (string): A string representing the name of the collection within the database where the articles will be stored.
mongoTimeout (int64): A duration in milliseconds that defines the timeout for a connection attempt to the MongoDB server. If the connection is not successful within this duration, it will time out.
delWorker (int): The number of worker goroutines to handle article deletions. It determines the level of concurrency for deletion operations.
delQueue (int): The size of the delete queue. It specifies how many delete requests can be buffered before the send operation blocks. If delQueue is 0 or negative, a default value will be used.
insWorker (int): The number of worker goroutines to handle article insertions. It determines the level of concurrency for insertion operations.
insQueue (int): The size of the insert queue. It specifies how many article insertion requests can be buffered before the send operation blocks. If insQueue is 0 or negative, a default value will be used.
getWorker (int): The number of worker goroutines to handle article reads. It determines the level of concurrency for read operations.
getQueue (int): The size of the read queue. It specifies how many read requests can be buffered before the send operation blocks. If getQueue is 0 or negative, a default value will be used.
testAfterInsert (bool): A flag indicating whether to perform a test after an article insertion. The specific test details are not provided in the function, and the flag can be used for application-specific testing purposes.
```


# How Load_MongoDB works
```
MongoDB Connection: The Load_MongoDB function establishes a connection to the MongoDB server specified in the mongoURI.

Database and Collections: It uses the dbName to select or create the target database for article storage and retrieval. Within the selected database, the function creates or retrieves the MongoDB collections for storing articles.

Initialization: The function initializes the provided delWorker, insWorker, and getWorker instances to handle article deletions, insertions, and read operations, respectively.

Start Workers: The function starts the provided delWorker, insWorker, and getWorker goroutines to process article deletions, insertions, and reading operations concurrently.

Queue Setup: The function sets up the delQueue, insQueue, and getQueue, which are channels used for communication between the main program and the worker goroutines.

Article Deletions: Articles to be deleted will be enqueued in the delQueue with their corresponding hashes, which will be processed by the delete worker goroutines.

Article Insertions: Articles to be inserted will be enqueued in the insQueue as MongoArticle instances, which will be processed by the insert worker goroutines. The testAfterInsert flag controls whether a test is performed after each article insertion. Unfortunately, the specific details of this test are not provided in the code.

Read (Get) Operations: Read requests for articles will be enqueued in the getQueue, which will be processed by the reader worker goroutines. This allows concurrent reading of articles from the MongoDB collections.

You can adjust the number of worker goroutines and queue sizes based on your application's requirements and the available resources.
```

# Usage Load_MongoDB

The Load_MongoDB function is typically called at the start of the program to set up the MongoDB configuration and prepare the worker goroutines for article storage and deletion.

After calling this function, the main program can use the delQueue and insQueue channels to enqueue articles for deletion or insertion, respectively.


# DefaultDelQueue

`DefaultDelQueue` is a channel used to handle the deletion of articles from MongoDB.

It is typically used to pass MessageIDHashes of articles that need to be deleted.

When an article needs to be deleted from MongoDB, its corresponding MessageIDHash is sent into the `DefaultDelQueue`.

The deletion process is usually handled by a separate goroutine (worker) that listens to the `DefaultDelQueue` and performs the actual deletion operation in the background.

Developers can customize this queue if they need to implement their own deletion logic or prioritize deletion operations differently.


# DefaultDelWorker

`DefaultDelWorker` is a goroutine (worker) responsible for processing deletion operations from the `DefaultDelQueue`.

It waits for MessageIDHashes to be sent into the `DefaultDelQueue` and performs the deletion operation for each received MessageIDHash.

The number of `DefaultDelWorker` goroutines can be adjusted based on the desired concurrency and performance requirements.

Having multiple workers handling deletion can improve the deletion throughput, especially when dealing with a large number of articles.


# DefaultInsQueue

`DefaultInsQueue` is a channel used to handle the insertion of articles into MongoDB.

It is typically used to pass `MongoArticle` objects that represent the articles to be inserted.

When a new article needs to be inserted into MongoDB, the `MongoArticle` object is sent into the `DefaultInsQueue`.

The insertion process is usually handled by a separate goroutine (worker) that listens to the `DefaultInsQueue` and performs the actual insertion operation in the background.

Developers can customize this queue if they need to implement their own insertion logic or prioritize insertion operations differently.


# DefaultInsWorker

`DefaultInsWorker` is a goroutine (worker) responsible for processing insertion operations from the `DefaultInsQueue`.

It waits for `MongoArticle` objects to be sent into the `DefaultInsQueue` and performs the insertion operation for each received `MongoArticle`.

The number of `DefaultInsWorker` goroutines can be adjusted based on the desired concurrency and performance requirements.

Having multiple workers handling insertion can improve the insertion throughput, especially when dealing with a large number of articles.

Overall, the `DefaultDelQueue`, `DefaultDelWorker`, `DefaultInsQueue`, and `DefaultInsWorker` are part of the infrastructure provided by the `mongostorage` package to efficiently manage the insertion and deletion of articles in a concurrent and asynchronous manner. They help in maximizing performance and scalability when dealing with a large number of articles in a MongoDB database. Developers can fine-tune the number of workers and customize the queues if needed to meet their specific use cases and requirements.


# MongoArticle Struct

The `MongoArticle` struct is part of the `mongostorage` package and represents an article to be stored in a MongoDB database. This struct holds various fields that represent different attributes of the article, such as its content, metadata, and compression-related information.

## Fields

1. `MessageIDHash`: This field stores the SHA256 hash of the `MessageID` field. The `MessageID` is a unique identifier for each article, and by storing its hash, the storage process can use it as an index to efficiently search for and identify articles in the MongoDB database.

2. `MessageID`: This field contains the unique identifier (Message-ID) of the article. It is typically in the format of an email address enclosed in angle brackets (e.g., `<example@example.com>`). The `MessageID` is used to uniquely identify each article and prevent duplicates.

3. `Head`: This field holds the header of the article, which contains metadata and information about the article, such as the author, subject, date, and newsgroups. The header is usually a series of lines, and in this case, it is stored as a byte slice.

4. `Body`: This field contains the main body of the article, which is the actual content of the article. Like the header, it is also stored as a byte slice.

5. `Headsize`: This field stores the size of the header in bytes. It is used to keep track of the size of the header after compression (if applicable) and is useful for reporting and analysis purposes.

6. `Bodysize`: This field stores the size of the body in bytes. Similar to `Headsize`, it is used to keep track of the size of the body after compression (if applicable).

The `MongoArticle` struct is used as a container to hold article data before it is inserted into the MongoDB database. Depending on the test cases and configurations in the code, the articles may or may not be compressed before insertion. The struct provides a flexible way to manage articles' content and metadata, making it suitable for storage and retrieval in MongoDB.


# MongoInsertWorker

The MongoInsertWorker is a component of the mongostorage package that handles the insertion of articles into a MongoDB database.

It operates as a worker goroutine, processing articles from a queue (Mongo_Insert_queue) and inserting them into the specified MongoDB collection.


## How MongoInsertWorker works

Initialization: Before starting the insertion process, the MongoInsertWorker is created and initialized.

It establishes a connection to the MongoDB database using the provided URI and database name.

Article Insertion: The worker listens to the Mongo_Insert_queue, which holds articles waiting to be inserted.

As soon as an article becomes available in the queue, the worker dequeues it and starts the insertion process.

Duplicate Check: Before inserting the article, the worker performs a duplicate check based on the MessageIDHash of the article.

It queries the MongoDB collection to check if an article with the same MessageIDHash already exists. This is to avoid inserting duplicate articles in the database.

Compression (Optional): Depending on the test case and configuration, the MongoInsertWorker may apply compression to the article's header and body before insertion.

If compression is required, it compresses the data using either gzip or zlib compression algorithms.

Insertion: After the duplicate check and optional compression, the worker inserts the article into the specified MongoDB collection.

Logging: During the insertion process, the worker logs relevant information such as the raw size of the article, the size after compression (if applied), and the success or failure of the insertion.


## MongoInsertWorker Concurrency

The MongoInsertWorker is designed to run as a goroutine.

To optimize the insertion process, multiple workers can be spawned concurrently, allowing for parallel processing of articles.

This concurrency can significantly improve the overall insertion performance, especially when dealing with a large number of articles.


## Usage MongoInsertWorker

The MongoInsertWorker is usually employed in conjunction with the MongoInsertQueue (which holds articles to be inserted) and the main program.

The main program enqueues articles into the Mongo_Insert_queue, and the MongoInsertWorker goroutine dequeues and processes them for insertion.


# MongoDeleteWorker
The MongoDeleteWorker is a key component of the mongostorage package responsible for handling the deletion of articles from a MongoDB database.

It operates as a worker goroutine, processing article hashes from a queue (Mongo_Delete_queue) and deleting the corresponding articles from the specified MongoDB collection.


## How MongoDeleteWorker works
Initialization: Before starting the deletion process, the MongoDeleteWorker is created and initialized.

It establishes a connection to the MongoDB database using the provided URI and database name.

Article Deletion: The worker listens to the Mongo_Delete_queue, which holds article hashes waiting to be deleted.

As soon as a hash becomes available in the queue, the worker dequeues it and starts the deletion process.

Article Lookup: The worker uses the provided article hash to query the MongoDB collection and find the article with the corresponding MessageIDHash.

Deletion: Once the article is located based on the MessageIDHash, the worker proceeds to delete it from the MongoDB collection.

Logging: During the deletion process, the worker logs relevant information such as the article hash being processed and whether the deletion was successful or encountered an error.


## MongoDeleteWorker Concurrency

The MongoDeleteWorker is designed to run as a goroutine. Multiple workers can be spawned concurrently to handle article deletions in parallel, which can improve overall deletion performance, especially for a large number of articles.


## Usage MongoDeleteWorker

The MongoDeleteWorker is typically used along with the MongoDeleteQueue (which holds article hashes to be deleted) and the main program. The main program enqueues article hashes into the Mongo_Delete_queue, and the MongoDeleteWorker goroutine dequeues and processes them for deletion.



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
