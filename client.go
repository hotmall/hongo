package hongo

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type Options struct {
	url string
}

var (
	client *mongo.Client
)

func init() {
	fmt.Println("start init")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	opts := Options{
		url: "mongodb://localhost:27017",
	}
	var err error
	client, err = mongo.Connect(ctx, options.Client().ApplyURI(opts.url))
	if err != nil {
		panic(err)
	}

	if err = client.Ping(ctx, readpref.Primary()); err != nil {
		panic(err)
	}
}

func Use(name string, opts ...*options.DatabaseOptions) *Database {
	return newDatabase(name, opts...)
}
