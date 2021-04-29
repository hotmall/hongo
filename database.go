package hongo

import (
	"context"
	"encoding/json"
	"errors"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var ErrNilDatabase = errors.New("database is nil")

type Database struct {
	db *mongo.Database
}

func newDatabase(name string, opts ...*options.DatabaseOptions) *Database {
	return &Database{
		db: client.Database(name, opts...),
	}
}

func (d *Database) Collection(name string, opts ...*options.CollectionOptions) *Collection {
	return newCollection(d.db, name, opts...)
}

// RunCommand executes the given command against the database. This function does not obey the Database's read
// preference. To specify a read preference, the RunCmdOptions.ReadPreference option must be used.
//
// The runCommand parameter must be a document for the command to be executed. It cannot be nil.
// This must be an order-preserving type such as bson.D. Map types such as bson.M are not valid.
// If the command document contains a session ID or any transaction-specific fields, the behavior is undefined.
//
// The opts parameter can be used to specify options for this operation (see the options.RunCmdOptions documentation).
func (d *Database) RunCommand(ctx context.Context, runCommand string, opts ...*options.RunCmdOptions) (map[string]interface{}, error) {

	var command bson.M
	if err := json.Unmarshal([]byte(runCommand), &command); err != nil {
		return nil, err
	}

	result := d.db.RunCommand(ctx, command, opts...)
	v := make(map[string]interface{})
	err := result.Decode(v)
	return v, err
}

func (d *Database) Drop(ctx context.Context) error {
	return d.db.Drop(ctx)
}

func (d *Database) ListCollections(ctx context.Context, filter string,
	opts ...*options.ListCollectionsOptions) ([]bson.M, error) {
	var f bson.M
	if err := json.Unmarshal([]byte(filter), &f); err != nil {
		return nil, err
	}

	cursor, err := d.db.ListCollections(ctx, f, opts...)
	if err != nil {
		return nil, err
	}

	var results []bson.M
	if err := cursor.All(ctx, &results); err != nil {
		return nil, err
	}

	return results, nil
}

func (d *Database) ListCollectionNames(ctx context.Context, filter string,
	opts ...*options.ListCollectionsOptions) ([]string, error) {
	var f bson.M
	if err := json.Unmarshal([]byte(filter), &f); err != nil {
		return nil, err
	}

	return d.db.ListCollectionNames(ctx, f, opts...)
}
