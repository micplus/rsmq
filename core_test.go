package rsmq_test

import (
	"testing"
	"time"

	"github.com/micplus/rsmq"

	"github.com/go-redis/redis/v9"
)

func TestProduceComsume(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr: ":6379",
	})

	// 生产者
	p := rsmq.NewProducer(rdb, 1000, true)
	stream, group := "testStream", "testGroup"
	if err := rsmq.GroupCreateMkStream(rdb, stream, group); err != nil {
		t.Error(err)
	}
	want := "hello,world"
	id, err := p.Add(stream, []byte(want))
	if err != nil {
		t.Error(err)
	}

	// 消费者
	consumer := "testConsumer"
	blockTime := 2 * time.Second
	minIdle := time.Second
	c := rsmq.NewConsumer(rdb, stream, group, consumer, blockTime, minIdle)
	got, err := c.Read()
	if err != nil {
		t.Error(err)
	}

	// 生产消费相同的消息
	if got.ID != id || string(got.Body) != want {
		t.Errorf("want id=%s,msg=%s but got id=%s,msg=%s", id, want, got.ID, got.Body)
	}
}
