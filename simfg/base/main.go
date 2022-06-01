package main

import (
	"context"
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"log"
	"time"
)

func main() {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatal("client error, ", err)
	}

	kv := clientv3.NewKV(cli)
	resp, err := kv.Put(context.Background(), "simfg01", "etcd01", clientv3.WithPrevKV())
	if err != nil {
		log.Fatal("put error, ", err)
	}
	fmt.Printf("resp: %+v", resp)
}
