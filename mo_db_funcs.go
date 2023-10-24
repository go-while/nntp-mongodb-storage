package mongostorage

import (
	"context"
	"errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
)

// InsertOneArticle is a function that inserts a single article into a MongoDB collection.
// function written by AI.
func InsertOneArticle(ctx context.Context, collection *mongo.Collection, article *MongoArticle) error {
	_, err := collection.InsertOne(ctx, article)
	if err != nil {
		log.Printf("Error collection.InsertOne err='%v'", err)
	}
	return err
} // end func InsertOneArticle

// InsertManyArticles is a function that performs a bulk insert of multiple articles into a MongoDB collection.
// function written by AI.
func InsertManyArticles(ctx context.Context, collection *mongo.Collection, articles []*MongoArticle) error {
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
		//logf(DEBUG, "Got an Error InsertManyArticles err='%v' inserted=%d", err, len(result.InsertedIDs))

		if retbool := IsDup(err); retbool {
			if result != nil {
				logf(DEBUG, "Info InsertManyArticles IsDup inserted=%d/%d", len(result.InsertedIDs), len(articles))
			 }
			// all insert errors are duplicates
			return nil
		} else {
			if result != nil {
				logf(DEBUG, "Warn InsertManyArticles IsDup inserted=%d/%d", len(result.InsertedIDs), len(articles))
			}
		}
		return err
	} else // end result InsertMany err != nil
	if len(result.InsertedIDs) == len(articles) {
		logf(DEBUG, "InsertManyArticles: inserted=%d/%d", len(result.InsertedIDs), len(articles))
	}
	return nil
} // end func InsertManyArticles

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
			logf(DEBUG, "Error, error is not mongo.WriteError")
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

// DeleteManyArticles is responsible for deleting multiple articles from the MongoDB collection based on a given set of MessageIDes.
// function written by AI.
func DeleteManyArticles(ctx context.Context, collection *mongo.Collection, messageIDs []string) (int64, error) {
	// Build the filter for DeleteMany
	filter := bson.M{
		"_id": bson.M{
			"$in": messageIDs,
		},
	}
	//logf(DEBUG, "DeleteManyArticles filter=%d", len(filter["_id"]))
	// Perform the DeleteMany operation
	result, err := collection.DeleteMany(ctx, filter)
	deleted := result.DeletedCount
	if err != nil {
		log.Printf("Error DeleteManyArticles deleted=%d err='%v'", deleted, err)
	} else if deleted > 0 {
		//logf(DEBUG, "DeleteManyArticles deleted=%d", deleted)
	}
	return deleted, err
} // end func DeleteManyArticles

// DeleteArticlesByMessageID deletes an article from the MongoDB collection by its MessageID.
// function written by AI.
func DeleteArticlesByMessageID(ctx context.Context, collection *mongo.Collection, messageID string) error {
	// Filter to find the articles with the given MessageID.
	filter := bson.M{"_id": messageID}

	// Delete the articles with the given MessageID.
	_, err := collection.DeleteMany(ctx, filter)
	if err != nil {
		return err
	}
	return nil
} // end func DeleteArticlesByMessageID

// RetrieveArticleByMessageID retrieves an article from the MongoDB collection by its MessageID.
// function written by AI.
func RetrieveArticleByMessageID(ctx context.Context, collection *mongo.Collection, messageID string) (*MongoArticle, error) {
	// Filter to find the article with the given MessageID.
	filter := bson.M{"_id": messageID}

	// Find the article in the collection.
	result := collection.FindOne(ctx, filter)

	// Check if the article exists.
	if result.Err() != nil {
		// Check if the error is due to "no documents in result".
		if result.Err() == mongo.ErrNoDocuments {
			logf(DEBUG, "Info RetrieveArticleByMessageID not found messageID='%s'", messageID)
			return nil, nil
		}
		// Return other errors as they indicate a problem with the query.
		return nil, result.Err()
	}

	// Decode the article from the BSON representation to a MongoArticle object.
	var article MongoArticle
	if err := result.Decode(&article); err != nil {
		log.Printf("Error RetrieveArticleByMessageID result.Decode err='%v'", err)
		return nil, err
	}

	return &article, nil
} // end func RetrieveArticleByMessageID

// RetrieveArticlesByMessageIDs is a function that retrieves articles from the MongoDB collection based on a list of MessageIDes.
// function written by AI.
func RetrieveArticlesByMessageIDs(ctx context.Context, collection *mongo.Collection, messageIDs []string) ([]*MongoArticle, error) {
	// Filter to find the articles with the given MessageIDes.
	filter := bson.M{"_id": bson.M{"$in": messageIDs}}

	// Find the articles in the collection.
	cursor, err := collection.Find(ctx, filter)
	if err != nil {
		log.Printf("Error RetrieveArticlesByMessageIDs coll.Find err='%v'", err)
		return nil, err
	}
	defer cursor.Close(ctx)

	// Decode the articles from the BSON representation to MongoArticle objects.
	var articles []*MongoArticle
	var founds []string
	for cursor.Next(ctx) {
		var article MongoArticle
		if err := cursor.Decode(&article); err != nil {
			log.Printf("Error RetrieveArticlesByMessageIDs cursor.Decode article err='%v'", err)
			return nil, err
		}
		article.Found = true
		articles = append(articles, &article)
		founds = append(founds, article.MessageID)
	}
	for _, messageID := range messageIDs {
		if !isStringInSlice(founds, messageID) {
			logf(DEBUG, "RetrieveArticlesByMessageIDs notfound messageID='%s'", messageID)
			article := &MongoArticle{}
			article.MessageID = messageID
			articles = append(articles, article)
		}
	}
	if err := cursor.Err(); err != nil {
		log.Printf("Error RetrieveArticlesByMessageIDs cursor.Err='%v'", err)
		return nil, err
	}

	return articles, nil
} // end func RetrieveArticlesByMessageIDs

// RetrieveHeadByMessageID is a function that retrieves the "Head" data of an article based on its MessageID.
// function written by AI.
func RetrieveHeadByMessageID(ctx context.Context, collection *mongo.Collection, messageID string) ([]byte, error) {
	// Filter to find the article with the given "messageID".
	filter := bson.M{"_id": messageID}

	// Projection to select only the "Head" field.
	projection := bson.M{"head": true, "hs": true, "enc": true}

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
} // end func retrieveHeadByMessageID

// RetrieveBodyByMessageID is a function that retrieves the "Body" data of an article  based on its MessageID.
// function written by AI.
func RetrieveBodyByMessageID(ctx context.Context, collection *mongo.Collection, messageID string) ([]byte, error) {
	// Filter to find the article with the given "messageID".
	filter := bson.M{"_id": messageID}

	// Projection to select only the "Body" field.
	projection := bson.M{"body": true, "bs": true, "enc": true}

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
} // end func retrieveBodyByMessageID

// CheckIfArticleExistsByMessageID checks if an article with the given MessageID exists in the MongoDB collection.
// function written by AI.
func CheckIfArticleExistsByMessageID(ctx context.Context, collection *mongo.Collection, messageID string) (bool, error) {
	// Filter to find the articles with the given MessageID.

	// This creates a projection document that specifies which fields should be included or excluded in the result.
	projection := bson.M{"head": false, "hs": true, "body": false, "bs": true, "enc": true}

	filter := bson.M{"_id": messageID}
	result := collection.FindOne(ctx, filter, options.FindOne().SetProjection(projection))
	if result.Err() != nil {
		// Check if the error is due to "no documents in result".
		if result.Err() == mongo.ErrNoDocuments {
			return false, nil
		}
		// Return other errors as they indicate a problem with the query.
		return false, result.Err()
	}
	// The document with the given MessageID exists in the collection.
	return true, nil
} // end func CheckIfArticleExistsByMessageID

// EOF mo_db_funcs.go
