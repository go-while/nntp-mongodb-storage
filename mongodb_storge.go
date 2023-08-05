package mongostorage

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"context"
	"encoding/gob"
	"fmt"
	//"github.com/go-while/go-utils"
	//"github.com/go-while/nntp-storage"
	//"github.com/go-while/nntp-overview"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"io/ioutil"
	"log"
	//"math/rand"
	//"sync"
	"time"
)

// Load_MongoDB initializes the MongoDB storage backend with the specified configuration parameters.
// The function starts the worker goroutines in separate background routines.
// function written by AI.
func Load_MongoDB(cfg *MongoStorageConfig) {
	//func Load_MongoDB(MongoURI string, MongoDatabaseName string, MongoCollection string, MongoTimeout int64, DelWorker int, DelQueue int, DelBatch int, InsWorker int, InsQueue int, InsBatch int, GetQueue int, GetWorker int, TestAfterInsert bool) {
	// Load_MongoDB initializes the mongodb storage backend
	SetDefaultsIfZero(cfg)

	Counter.Init() // = COUNTER{m: make(map[string]uint64) }

	Mongo_Delete_queue = make(chan string, cfg.DelQueue)
	Mongo_Insert_queue = make(chan MongoArticle, cfg.InsQueue)
	Mongo_Reader_queue = make(chan MongoReadRequest, cfg.GetQueue)
	log.Printf("Load_MongoDB: Reader GetQueue=%d GetWorker=%d", cfg.GetQueue, cfg.GetWorker)
	log.Printf("Load_MongoDB: Delete DelQueue=%d DelWorker=%d DelBatch=%d", cfg.DelQueue, cfg.DelWorker, cfg.DelBatch)
	log.Printf("Load_MongoDB: Insert InsQueue=%d InsWorker=%d InsBatch=%d", cfg.InsQueue, cfg.InsWorker, cfg.InsBatch)

	go workerStatus()
	go mongoWorker_UpDn_Scaler(cfg)
	for i := 1; i <= cfg.GetWorker; i++ {
		go mongoWorker_Reader(i, &READER, cfg)
	}
	for i := 1; i <= cfg.DelWorker; i++ {
		go mongoWorker_Delete(i, &DELETE, cfg)
	}
	for i := 1; i <= cfg.InsWorker; i++ {
		go mongoWorker_Insert(i, &INSERT, cfg)
	}

} // end func Load_MongoDB

// ConnectMongoDB is a function responsible for establishing a connection to the MongoDB server and accessing a specific collection.
// It takes the following parameters:
// - who: A string representing the name or identifier of the calling function or worker.
// function written by AI.
func ConnectMongoDB(who string, cfg *MongoStorageConfig) (context.Context, context.CancelFunc, *mongo.Client, *mongo.Collection, error) {

	client, err := mongo.NewClient(options.Client().ApplyURI(cfg.MongoURI))
	if err != nil {
		log.Printf("Error creating MongoDB client: %v", err)
		return nil, nil, nil, nil, err
	}

	// Set a timeout for the connection.
	newTimeout := time.Second * time.Duration(cfg.MongoTimeout)
	//deadline := time.Now().Add(newTimeout)
	//ctx, cancel := context.WithDeadline(context.Background(), deadline)
	ctx, cancel := context.WithTimeout(context.Background(), newTimeout)
	err = client.Connect(ctx)
	if err != nil {
		log.Printf("Error connecting to MongoDB: %v", err)
		cancel()
		return nil, nil, nil, nil, err
	}

	// Access the MongoDB collection.
	collection := client.Database(cfg.MongoDatabaseName).Collection(cfg.MongoCollection)

	log.Printf("-> ConnectMongoDB who=%s", who)
	return ctx, cancel, client, collection, err
} // end func ConnectMongoDB

// DisConnectMongoDB is a function responsible for disconnecting from the MongoDB server.
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

// sliceContains checks if a given target string exists in the provided slice of strings.
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
// function written by AI.
func isPStringInSlice(slice []*string, target *string) bool {
	for _, s := range slice {
		if s != nil && target != nil && *s == *target {
			return true
		}
	}
	return false
} // end func isPStringInSlice

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

// EOF mongodb_storage.go : package mongostorage
