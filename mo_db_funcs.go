package mongostorage

import (
	"context"
	"errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
)

// MongoInsertOneArticle is a function that inserts a single article into a MongoDB collection.
// function written by AI.
func MongoInsertOneArticle(ctx context.Context, collection *mongo.Collection, article *MongoArticle) error {
	_, err := collection.InsertOne(ctx, article)
	if err != nil {
		log.Printf("Error collection.InsertOne err='%v'", err)
	}
	return err
} // end func MongoInsertOneArticle

// MongoInsertManyArticles is a function that performs a bulk insert of multiple articles into a MongoDB collection.
// function written by AI.
func MongoInsertManyArticles(ctx context.Context, collection *mongo.Collection, articles []*MongoArticle) error {
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
		//log.Printf("Got an Error MongoInsertManyArticles err='%v' inserted=%d", err, len(result.InsertedIDs))

		if retbool := IsDup(err); retbool {
			log.Printf("Info MongoInsertManyArticles IsDup inserted=%d/%d", len(result.InsertedIDs), len(articles))
			// all insert errors are duplicates
			return nil
		} else {
			log.Printf("Warn MongoInsertManyArticles IsDup inserted=%d/%d", len(result.InsertedIDs), len(articles))
		}
		/*
			if writeErrors, ok := err.(mongo.writeErrors); ok {
				// Handle individual write errors for each document.
				for _, writeError := range writeErrors {
					if writeError.Code == 11000 { // Duplicate key error code
						// Handle duplicate key error here.
						log.Printf("Duplicate key error for document: Code=%d", writeError.Code)
						continue
					} else {
						// Handle other write errors, if needed.
						log.Printf("Error MongoInsertManyArticles Other insert error code=%d", writeError.Code)
						continue
					}
				}
			} else {
				// Handle general connection or other error.
				log.Printf("Error MongoInsertManyArticles err='%v'", err)
				return err
			}
		*/
		return err
	} else // end result InsertMany err != nil
	if len(result.InsertedIDs) == len(articles) {
		log.Printf("MongoInsertManyArticles: inserted=%d/%d", len(result.InsertedIDs), len(articles))
	}
	return nil
} // end func MongoInsertManyArticles

// IsDuplicateKeyError returns true if err is a duplicate key error
func IsDup(err error) bool {
	retbool := false
	for ; err != nil; err = errors.Unwrap(err) {
		if e, ok := err.(mongo.ServerError); ok {
			if e.HasErrorCode(11000) {
				retbool = true
			}
			/*
				if e.HasErrorCode(11001) {
					dupes++
				}
				if e.HasErrorCode(12582) {
					dupes++
				}*/
			/*
				return e.HasErrorCode(11000) || e.HasErrorCode(11001) || e.HasErrorCode(12582) ||
					e.HasErrorCodeWithMessage(16460, " E11000 ")
			*/
		} else {
			log.Printf("Error, error is not mongo.WriteError")
		}
	}
	return retbool
} // end func IsDuplicateKeyError

/*
func IsDup(err error) (bool, int) {
	dupes := 0
	var e mongo.WriteException
	//var e mongo.WriteErrors
	if errors.As(err, &e) {
		for _, we := range e.WriteErrors {
			if we.Code == 11000 {
				dupes++
				//return true
			}
		}
	}
	if dupes == 0 {
		return false, 0
	}
	return true, dupes
} // end func IsDup
*/

// MongoDeleteManyArticles is responsible for deleting multiple articles from the MongoDB collection based on a given set of MessageIDHashes.
// function written by AI.
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
		log.Printf("Error MongoDeleteManyArticles err='%v'", err)
		return false
	}

	log.Printf("MongoDB Deleted many=%d", result.DeletedCount)
	return result.DeletedCount == int64(len(msgidhashes))
} // end func MongoDeleteManyArticles

// DeleteArticlesByMessageIDHash deletes an article from the MongoDB collection by its MessageIDHash.
// function written by AI.
func DeleteArticlesByMessageIDHash(ctx context.Context, collection *mongo.Collection, messageIDHash string) error {
	// Filter to find the articles with the given MessageIDHash.
	filter := bson.M{"_id": messageIDHash}

	// Delete the articles with the given MessageIDHash.
	_, err := collection.DeleteMany(ctx, filter)
	if err != nil {
		return err
	}
	return nil
} // end func DeleteArticlesByMessageIDHash

// RetrieveArticleByMessageIDHash retrieves an article from the MongoDB collection by its MessageIDHash.
// function written by AI.
func RetrieveArticleByMessageIDHash(ctx context.Context, collection *mongo.Collection, messageIDHash string) (*MongoArticle, error) {
	// Filter to find the article with the given MessageIDHash.
	filter := bson.M{"_id": messageIDHash}

	// Find the article in the collection.
	result := collection.FindOne(ctx, filter)

	// Check if the article exists.
	if result.Err() != nil {
		// Check if the error is due to "no documents in result".
		if result.Err() == mongo.ErrNoDocuments {
			log.Printf("Info RetrieveArticleByMessageIDHash not found hash=%s", messageIDHash)
			return nil, nil
		}
		// Return other errors as they indicate a problem with the query.
		return nil, result.Err()
	}

	// Decode the article from the BSON representation to a MongoArticle object.
	var article MongoArticle
	if err := result.Decode(&article); err != nil {
		log.Printf("Error RetrieveArticleByMessageIDHash result.Decode err='%v'", err)
		return nil, err
	}

	return &article, nil
} // end func RetrieveArticleByMessageIDHash

// ReadArticlesByMessageIDHashes is a function that retrieves articles from the MongoDB collection based on a list of MessageIDHashes.
// function written by AI.
func ReadArticlesByMessageIDHashes(ctx context.Context, collection *mongo.Collection, msgidhashes []*string) ([]*MongoArticle, error) {
	// Filter to find the articles with the given MessageIDHashes.
	filter := bson.M{"_id": bson.M{"$in": msgidhashes}}

	// Find the articles in the collection.
	cursor, err := collection.Find(ctx, filter)
	if err != nil {
		log.Printf("Error ReadArticlesByMessageIDHashes coll.Find err='%v'", err)
		return nil, err
	}
	defer cursor.Close(ctx)

	// Decode the articles from the BSON representation to MongoArticle objects.
	var articles []*MongoArticle
	var founds []*string
	for cursor.Next(ctx) {
		var article MongoArticle
		if err := cursor.Decode(&article); err != nil {
			log.Printf("Error ReadArticlesByMessageIDHashes cursor.Decode article err='%v'", err)
			return nil, err
		}
		article.Found = true
		articles = append(articles, &article)
		founds = append(founds, article.MessageIDHash)
	}
	for _, hash := range msgidhashes {
		if !isPStringInSlice(founds, hash) {
			//log.Printf("ReadArticlesByMessageIDHashes notfound hash='%s'", *hash)
			var article MongoArticle
			article.MessageIDHash = hash
			articles = append(articles, &article)
		}
	}
	if err := cursor.Err(); err != nil {
		log.Printf("Error ReadArticlesByMessageIDHashes cursor.Err='%v'", err)
		return nil, err
	}

	return articles, nil
} // end func ReadArticlesByMessageIDHashes

// RetrieveHeadByMessageIDHash is a function that retrieves the "Head" data of an article based on its MessageIDHash.
// function written by AI.
func RetrieveHeadByMessageIDHash(ctx context.Context, collection *mongo.Collection, messageIDHash string) ([]byte, error) {
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

// RetrieveBodyByMessageIDHash is a function that retrieves the "Body" data of an article  based on its MessageIDHash.
// function written by AI.
func RetrieveBodyByMessageIDHash(ctx context.Context, collection *mongo.Collection, messageIDHash string) ([]byte, error) {
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

// RetrieveBodyByMessageIDHash is a function that retrieves the "Body" data of an article based on its MessageIDHash.
// function written by AI.
func CheckIfArticleExistsByMessageIDHash(ctx context.Context, collection *mongo.Collection, messageIDHash *string) (bool, error) {
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
} // end func CheckIfArticleExistsByMessageIDHash
