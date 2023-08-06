package mongostorage

// GetDefaultMongoStorageConfig returns a MongoStorageConfig with default values.
func GetDefaultMongoStorageConfig() MongoStorageConfig {
	cfg := MongoStorageConfig{

		// Default MongoDB connection URI
		MongoURI: DefaultMongoURI,

		// Default MongoDB database name
		MongoDatabaseName: DefaultMongoDatabaseName,

		// Default MongoDB collection name
		MongoCollection: DefaultMongoCollection,

		// Default MongoDB timeout (in seconds) for connecting to the server
		MongoTimeout: DefaultMongoTimeout,

		// Default size of the delete queue (number of delete requests buffered before blocking)
		DelQueue: DefaultDelQueue,

		// Default number of delete worker goroutines
		DelWorker: DefaultDelWorker,

		// Default number of Msgidhashes a DeleteWorker will cache before deleting to batch into one process
		DelBatch: DefaultDeleteBatchSize,

		// Default size of the insert queue (number of insertion requests buffered before blocking)
		InsQueue: DefaultInsQueue,

		// Default number of insert worker goroutines
		InsWorker: DefaultInsWorker,

		// Default number of Articles an InsertWorker will cache before inserting to batch into one process
		InsBatch: DefaultInsertBatchSize,

		// Default size of the read queue (number of read requests buffered before blocking)
		GetQueue: DefaultGetQueue,

		// Default number of reader worker goroutines
		GetWorker: DefaultGetWorker,

		// Default ms timer for Insert / Delete: will wait up to N millisec before flushing batched to mongodb
		FlushTimer: DefaultFlushTimer,

		// Default test flag for performing a test after an article insertion
		TestAfterInsert: false, // You can change this to true if you want to perform a test after an article insertion.
	}

	return cfg
} // end func GetDefaultMongoStorageConfig

// SetDefaultsIfZero takes a pointer to a MongoStorageConfig and sets integer-typed fields to their default values
// if their current value is less than or equal to zero. It also sets default values for empty strings.
// This function is useful for ensuring that all the fields in the MongoStorageConfig have valid values.
func SetDefaultsIfZero(cfg *MongoStorageConfig) {
	if cfg == nil {
		return
	}

	if cfg.MongoURI == "" {
		cfg.MongoURI = DefaultMongoURI
	}
	if cfg.MongoDatabaseName == "" {
		cfg.MongoDatabaseName = DefaultMongoDatabaseName
	}
	if cfg.MongoCollection == "" {
		cfg.MongoCollection = DefaultMongoCollection
	}
	if cfg.MongoTimeout <= 0 {
		cfg.MongoTimeout = DefaultMongoTimeout
	}
	if cfg.DelQueue <= 0 {
		cfg.DelQueue = DefaultDelQueue
	}
	if cfg.DelWorker <= 0 {
		cfg.DelWorker = DefaultDelWorker
	}
	if cfg.DelBatch <= 0 {
		cfg.DelBatch = DefaultDeleteBatchSize
	}
	if cfg.InsQueue <= 0 {
		cfg.InsQueue = DefaultInsQueue
	}
	if cfg.InsWorker <= 0 {
		cfg.InsWorker = DefaultInsWorker
	}
	if cfg.InsBatch <= 0 {
		cfg.InsBatch = DefaultInsertBatchSize
	}
	if cfg.GetQueue <= 0 {
		cfg.GetQueue = DefaultGetQueue
	}
	if cfg.GetWorker <= 0 {
		cfg.GetWorker = DefaultGetWorker
	}
} // end func SetDefaultsIfZero
