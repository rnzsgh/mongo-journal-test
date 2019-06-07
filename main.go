package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"

	log "github.com/golang/glog"
)

func init() {
	flag.Parse()
	flag.Lookup("logtostderr").Value.Set("true")
}

func main() {

	log.Flush()

	client, err := db()
	if err != nil {
		panic(err)
	}

	if err = client.Database("work").Collection("test").Drop(context.TODO()); err != nil {
		log.Errorf("Unable to drop collection work.test: %v", err)
		return
	}

	start := time.Now()

	for i := 0; i < 1000; i++ {
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		if _, err = client.Database("work").Collection("test").InsertOne(ctx, bson.M{"name": "pi", "value": 3.14159}); err != nil {

		}
	}

	t := time.Now()
	elapsed := t.Sub(start)

	log.Infof("Time: %v", elapsed)

}

func db() (*mongo.Client, error) {

	endpoint := "localhost"
	port := 27017
	user := "test"
	password := "test"
	caFile := "local.pem"

	journal := true

	connectionUri := fmt.Sprintf(
		"mongodb://%s:%s@%s:%d/work?ssl=true&sslCertificateAuthorityFile=%s&sslInsecure=true",
		user,
		password,
		endpoint,
		port,
		caFile,
	)

	client, err := mongo.NewClient(
		options.Client().ApplyURI(connectionUri),
		options.Client().SetWriteConcern(writeconcern.New(writeconcern.J(journal))),
	)

	if err != nil {
		return nil, fmt.Errorf("Unable to create new db client - endpoint: %s - reason: %v", endpoint, err)
	}

	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	if err = client.Connect(ctx); err != nil {
		return nil, fmt.Errorf("Unable to connect to db - endpoint: %s - reason: %v", endpoint, err)
	}

	return client, nil
}
