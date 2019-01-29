package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/mongo"
)

func main() {
	client, err := mongo.NewClient("mongodb://localhost:27017")
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = client.Connect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	dbs, err := client.ListDatabaseNames(ctx, bson.M{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(dbs)

	user := client.Database("example").Collection("user")

	//INSERT
	one := bson.M{"name": "abc", "number": 123}
	two := bson.M{"name": "def", "url": "abcdef"}
	insertResult, err := user.InsertOne(ctx, one)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Inserted a single document: ", insertResult.InsertedID)

	records := []interface{}{one, two}
	insertManyResult, err := user.InsertMany(ctx, records)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Inserted multiple documents: ", insertManyResult.InsertedIDs)

	//UPDATE
	filter := bson.M{"name": "abc"}
	update := bson.M{"$set": bson.M{"number": 999}}
	_, err = user.UpdateMany(ctx, filter, update)
	if err != nil {
		log.Fatal(err)
	}

	//SELECT
	cur, err := user.Find(ctx, bson.M{})
	if err != nil {
		log.Fatal(err)
	}
	for cur.Next(ctx) {
		var result bson.M
		err := cur.Decode(&result)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(result)
	}

	//DELETE
	deleteResult, err := user.DeleteMany(ctx, bson.M{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Deleted %v records\n", deleteResult.DeletedCount)
}
