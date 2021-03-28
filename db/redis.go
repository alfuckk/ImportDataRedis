package db

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

type Db struct {
	Addr     string
	Password string
	DB       int
	PoolSize string
}

var rdb *redis.Client
var ctx = context.Background()

func (db Db) ConnDb(dbNum int) (err error) {
	rdb = redis.NewClient(&redis.Options{
		Addr:     db.Addr,
		Password: db.Password, // no password set
		DB:       dbNum,       // use default DB
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = rdb.Ping(ctx).Result()
	return err
}

func (db Db) Set(i string, data map[string]interface{}) error {
	mJSON, err := json.Marshal(data)
	if err != nil {
		fmt.Println(string(mJSON))
	}
	err = rdb.Set(ctx, "qq_mobile:"+i, mJSON, 0).Err()
	if err != nil {
		panic(err)
	}
	return nil
}
