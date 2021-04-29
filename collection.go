package hongo

import (
	"context"
	"encoding/json"
	"errors"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var ErrNilCollection = errors.New("collection is nil")

type Collection struct {
	coll *mongo.Collection
}

func newCollection(db *mongo.Database, name string, opts ...*options.CollectionOptions) *Collection {
	return &Collection{
		coll: db.Collection(name, opts...),
	}
}

func (c *Collection) CountDocuments(ctx context.Context, filter string, opts ...*options.CountOptions) (int64, error) {
	var f bson.M
	if err := json.Unmarshal([]byte(filter), &f); err != nil {
		return 0, err
	}
	return c.coll.CountDocuments(ctx, f, opts...)
}

func (c *Collection) DeleteMany(ctx context.Context, filter string, opts ...*options.DeleteOptions) (*mongo.DeleteResult, error) {
	var f bson.M
	if err := json.Unmarshal([]byte(filter), &f); err != nil {
		return nil, err
	}
	return c.coll.DeleteMany(ctx, f, opts...)
}

func (c *Collection) DeleteOne(ctx context.Context, filter string, opts ...*options.DeleteOptions) (*mongo.DeleteResult, error) {
	var f bson.M
	if err := json.Unmarshal([]byte(filter), &f); err != nil {
		return nil, err
	}
	return c.coll.DeleteOne(ctx, f, opts...)
}

// Distinct executes a distinct command to find the unique values for a specified field in the collection.
//
// The fieldName parameter specifies the field name for which distinct values should be returned.
//
// The filter parameter must be a document containing query operators and can be used to select which documents are
// considered. It cannot be nil. An empty document (e.g. bson.D{}) should be used to select all documents.
//
// The opts parameter can be used to specify options for the operation (see the options.DistinctOptions documentation).
//
// For more information about the command, see https://docs.mongodb.com/manual/reference/command/distinct/.
func (c *Collection) Distinct(ctx context.Context, fieldName string, filter string,
	opts ...*options.DistinctOptions) ([]interface{}, error) {

	var f bson.M
	if err := json.Unmarshal([]byte(filter), &f); err != nil {
		return nil, err
	}
	return c.coll.Distinct(ctx, fieldName, f, opts...)
}

func (c *Collection) Drop(ctx context.Context) error {
	return c.coll.Drop(ctx)
}

func (c *Collection) EstimatedDocumentCount(ctx context.Context, opts ...*options.EstimatedDocumentCountOptions) (int64, error) {
	return c.coll.EstimatedDocumentCount(ctx, opts...)
}

// Find executes a find command and returns a Cursor over the matching documents in the collection.
//
// The filter parameter must be a document containing query operators and can be used to select which documents are
// included in the result. It cannot be nil. An empty document (e.g. bson.D{}) should be used to include all documents.
//
// The opts parameter can be used to specify options for the operation (see the options.FindOptions documentation).
//
// For more information about the command, see https://docs.mongodb.com/manual/reference/command/find/.
func (c *Collection) Find(ctx context.Context, filter string, opts ...*options.FindOptions) ([]bson.M, error) {
	var f bson.M
	if err := json.Unmarshal([]byte(filter), &f); err != nil {
		return nil, err
	}
	cursor, err := c.coll.Find(ctx, f, opts...)
	if err != nil {
		return nil, err
	}

	var results []bson.M
	if err := cursor.All(ctx, &results); err != nil {
		return nil, err
	}

	return results, nil
}

// FindOne executes a find command and returns a SingleResult for one document in the collection.
//
// The filter parameter must be a document containing query operators and can be used to select the document to be
// returned. It cannot be nil. If the filter does not match any documents, a SingleResult with an error set to
// ErrNoDocuments will be returned. If the filter matches multiple documents, one will be selected from the matched set.
//
// The opts parameter can be used to specify options for this operation (see the options.FindOneOptions documentation).
//
// For more information about the command, see https://docs.mongodb.com/manual/reference/command/find/.
func (c *Collection) FindOne(ctx context.Context, filter string, opts ...*options.FindOneOptions) (map[string]interface{}, error) {
	if c.coll == nil {
		return nil, ErrNilCollection
	}

	var f bson.M
	if err := json.Unmarshal([]byte(filter), &f); err != nil {
		return nil, err
	}

	result := c.coll.FindOne(ctx, f, opts...)
	v := make(map[string]interface{})
	err := result.Decode(v)
	return v, err
}

// FindOneAndDelete executes a findAndModify command to delete at most one document in the collection. and returns the
// document as it appeared before deletion.
//
// The filter parameter must be a document containing query operators and can be used to select the document to be
// deleted. It cannot be nil. If the filter does not match any documents, a SingleResult with an error set to
// ErrNoDocuments wil be returned. If the filter matches multiple documents, one will be selected from the matched set.
//
// The opts parameter can be used to specify options for the operation (see the options.FindOneAndDeleteOptions
// documentation).
//
// For more information about the command, see https://docs.mongodb.com/manual/reference/command/findAndModify/.
func (c *Collection) FindOneAndDelete(ctx context.Context, filter string,
	opts ...*options.FindOneAndDeleteOptions) (map[string]interface{}, error) {
	var f bson.M
	if err := json.Unmarshal([]byte(filter), &f); err != nil {
		return nil, err
	}

	result := c.coll.FindOneAndDelete(ctx, f, opts...)
	v := make(map[string]interface{})
	err := result.Decode(v)
	return v, err
}

// FindOneAndReplace executes a findAndModify command to replace at most one document in the collection
// and returns the document as it appeared before replacement.
//
// The filter parameter must be a document containing query operators and can be used to select the document to be
// replaced. It cannot be nil. If the filter does not match any documents, a SingleResult with an error set to
// ErrNoDocuments wil be returned. If the filter matches multiple documents, one will be selected from the matched set.
//
// The replacement parameter must be a document that will be used to replace the selected document. It cannot be nil
// and cannot contain any update operators (https://docs.mongodb.com/manual/reference/operator/update/).
//
// The opts parameter can be used to specify options for the operation (see the options.FindOneAndReplaceOptions
// documentation).
//
// For more information about the command, see https://docs.mongodb.com/manual/reference/command/findAndModify/.
func (c *Collection) FindOneAndReplace(ctx context.Context, filter string, replacement string,
	opts ...*options.FindOneAndReplaceOptions) (map[string]interface{}, error) {
	var f bson.M
	if err := json.Unmarshal([]byte(filter), &f); err != nil {
		return nil, err
	}

	var r bson.M
	if err := json.Unmarshal([]byte(replacement), &r); err != nil {
		return nil, err
	}
	result := c.coll.FindOneAndReplace(ctx, f, r, opts...)
	v := make(map[string]interface{})
	err := result.Decode(v)
	return v, err
}

// FindOneAndUpdate executes a findAndModify command to update at most one document in the collection and returns the
// document as it appeared before updating.
//
// The filter parameter must be a document containing query operators and can be used to select the document to be
// updated. It cannot be nil. If the filter does not match any documents, a SingleResult with an error set to
// ErrNoDocuments wil be returned. If the filter matches multiple documents, one will be selected from the matched set.
//
// The update parameter must be a document containing update operators
// (https://docs.mongodb.com/manual/reference/operator/update/) and can be used to specify the modifications to be made
// to the selected document. It cannot be nil or empty.
//
// The opts parameter can be used to specify options for the operation (see the options.FindOneAndUpdateOptions
// documentation).
//
// For more information about the command, see https://docs.mongodb.com/manual/reference/command/findAndModify/.
func (c *Collection) FindOneAndUpdate(ctx context.Context, filter string, update string,
	opts ...*options.FindOneAndUpdateOptions) (map[string]interface{}, error) {
	var f bson.M
	if err := json.Unmarshal([]byte(filter), &f); err != nil {
		return nil, err
	}

	var u bson.M
	if err := json.Unmarshal([]byte(update), &u); err != nil {
		return nil, err
	}
	result := c.coll.FindOneAndUpdate(ctx, f, u, opts...)
	v := make(map[string]interface{})
	err := result.Decode(v)
	return v, err
}

// func (c *Collection) Indexes() IndexView {
// 	return nil
// }

func (c *Collection) InsertMany(ctx context.Context, documents string,
	opts ...*options.InsertManyOptions) (*mongo.InsertManyResult, error) {
	var a bson.A
	if err := json.Unmarshal([]byte(documents), &a); err != nil {
		return nil, err
	}
	return c.coll.InsertMany(ctx, a, opts...)
}

func (c *Collection) InsertOne(ctx context.Context, document string,
	opts ...*options.InsertOneOptions) (*mongo.InsertOneResult, error) {
	var d bson.M
	if err := json.Unmarshal([]byte(document), &d); err != nil {
		return nil, err
	}
	return c.coll.InsertOne(ctx, d, opts...)
}

func (c *Collection) Name() string {
	return c.coll.Name()
}

// ReplaceOne executes an update command to replace at most one document in the collection.
//
// The filter parameter must be a document containing query operators and can be used to select the document to be
// replaced. It cannot be nil. If the filter does not match any documents, the operation will succeed and an
// UpdateResult with a MatchedCount of 0 will be returned. If the filter matches multiple documents, one will be
// selected from the matched set and MatchedCount will equal 1.
//
// The replacement parameter must be a document that will be used to replace the selected document. It cannot be nil
// and cannot contain any update operators (https://docs.mongodb.com/manual/reference/operator/update/).
//
// The opts parameter can be used to specify options for the operation (see the options.ReplaceOptions documentation).
//
// For more information about the command, see https://docs.mongodb.com/manual/reference/command/update/.
func (c *Collection) ReplaceOne(ctx context.Context, filter string, replacement string,
	opts ...*options.ReplaceOptions) (*mongo.UpdateResult, error) {

	var f bson.M
	if err := json.Unmarshal([]byte(filter), &f); err != nil {
		return nil, err
	}

	var r bson.M
	if err := json.Unmarshal([]byte(replacement), &r); err != nil {
		return nil, err
	}

	return c.coll.ReplaceOne(ctx, f, r, opts...)
}

// UpdateMany executes an update command to update documents in the collection.
//
// The filter parameter must be a document containing query operators and can be used to select the documents to be
// updated. It cannot be nil. If the filter does not match any documents, the operation will succeed and an UpdateResult
// with a MatchedCount of 0 will be returned.
//
// The update parameter must be a document containing update operators
// (https://docs.mongodb.com/manual/reference/operator/update/) and can be used to specify the modifications to be made
// to the selected documents. It cannot be nil or empty.
//
// The opts parameter can be used to specify options for the operation (see the options.UpdateOptions documentation).
//
// For more information about the command, see https://docs.mongodb.com/manual/reference/command/update/.
func (c *Collection) UpdateMany(ctx context.Context, filter string, update string,
	opts ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
	var f bson.M
	if err := json.Unmarshal([]byte(filter), &f); err != nil {
		return nil, err
	}

	var u bson.M
	if err := json.Unmarshal([]byte(update), &u); err != nil {
		return nil, err
	}

	return c.coll.UpdateMany(ctx, f, u, opts...)
}

// UpdateOne executes an update command to update at most one document in the collection.
//
// The filter parameter must be a document containing query operators and can be used to select the document to be
// updated. It cannot be nil. If the filter does not match any documents, the operation will succeed and an UpdateResult
// with a MatchedCount of 0 will be returned. If the filter matches multiple documents, one will be selected from the
// matched set and MatchedCount will equal 1.
//
// The update parameter must be a document containing update operators
// (https://docs.mongodb.com/manual/reference/operator/update/) and can be used to specify the modifications to be
// made to the selected document. It cannot be nil or empty.
//
// The opts parameter can be used to specify options for the operation (see the options.UpdateOptions documentation).
//
// For more information about the command, see https://docs.mongodb.com/manual/reference/command/update/.
func (c *Collection) UpdateOne(ctx context.Context, filter string, update string,
	opts ...*options.UpdateOptions) (*mongo.UpdateResult, error) {

	var f bson.M
	if err := json.Unmarshal([]byte(filter), &f); err != nil {
		return nil, err
	}

	var u bson.M
	if err := json.Unmarshal([]byte(update), &u); err != nil {
		return nil, err
	}
	return c.coll.UpdateOne(ctx, f, u, opts...)
}

// UpdateByID executes an update command to update the document whose _id value matches the provided ID in the collection.
// This is equivalent to running UpdateOne(ctx, bson.D{{"_id", id}}, update, opts...).
//
// The id parameter is the _id of the document to be updated. It cannot be nil. If the ID does not match any documents,
// the operation will succeed and an UpdateResult with a MatchedCount of 0 will be returned.
//
// The update parameter must be a document containing update operators
// (https://docs.mongodb.com/manual/reference/operator/update/) and can be used to specify the modifications to be
// made to the selected document. It cannot be nil or empty.
//
// The opts parameter can be used to specify options for the operation (see the options.UpdateOptions documentation).
//
// For more information about the command, see https://docs.mongodb.com/manual/reference/command/update/.
func (c *Collection) UpdateByID(ctx context.Context, id interface{}, update string,
	opts ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
	if id == nil {
		return nil, mongo.ErrNilValue
	}

	var u bson.M
	if err := json.Unmarshal([]byte(update), &u); err != nil {
		return nil, err
	}

	return c.coll.UpdateOne(ctx, bson.D{{"_id", id}}, u, opts...)
}
