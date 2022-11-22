// redis stream message queue.
package rsmq

import (
	"context"
	"time"

	"github.com/go-redis/redis/v9"
)

type Message struct {
	ID   string
	Body []byte
}

type Producer struct {
	cli    *redis.Client
	maxLen int64
	approx bool
}

func NewProducer(cli *redis.Client, maxLen int64, approx bool) *Producer {
	return &Producer{
		cli:    cli,
		maxLen: maxLen,
		approx: approx,
	}
}

func (p *Producer) Add(stream string, msg []byte) (string, error) {
	return p.cli.XAdd(context.Background(), &redis.XAddArgs{
		Stream: stream,
		MaxLen: p.maxLen,
		Approx: p.approx,
		ID:     "*",
		Values: []any{"body", msg},
	}).Result()
}

type Consumer struct {
	cli     *redis.Client
	stream  string
	group   string
	name    string
	block   time.Duration
	minIdle time.Duration
	start   string
}

func NewConsumer(cli *redis.Client, stream, group, name string, block, minIdle time.Duration) *Consumer {
	return &Consumer{
		cli:     cli,
		stream:  stream,
		group:   group,
		name:    name,
		block:   block,
		minIdle: minIdle,
		start:   "0-0",
	}
}

// 仅支持单条数据
func (c *Consumer) Read() (*Message, error) {
	// Block直到读出数据
	res, err := c.cli.XReadGroup(context.Background(), &redis.XReadGroupArgs{
		Group:    c.group,
		Streams:  []string{c.stream, ">"},
		Consumer: c.name,
		Block:    c.block,
		Count:    1,
	}).Result()
	if err != nil && err != redis.Nil { // 阻塞超时返回(nil)
		return nil, err
	}
	if err == nil && len(res) > 0 {
		msg := res[0].Messages[0]
		return &Message{
			ID:   msg.ID,
			Body: []byte(msg.Values["body"].(string)),
		}, nil
	}
	// Block没有读出数据，则认为没有新数据，检查旧数据是否有超时未ACK。
	resAC, start, err := c.cli.XAutoClaim(context.Background(), &redis.XAutoClaimArgs{
		Stream:   c.stream,
		Group:    c.group,
		Consumer: c.name,
		Count:    1,
		MinIdle:  c.minIdle,
		Start:    c.start,
	}).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}
	c.start = start
	if err != nil || len(resAC) == 0 { // 真的没消息
		return nil, nil
	}
	msg := resAC[0]
	return &Message{
		ID:   msg.ID,
		Body: []byte(msg.Values["body"].(string)),
	}, nil
}

// 随着业务需求拓展出的新功能。
// 从队列持久化到数据库，实时性要求低，通过大批量操作提高性能。
// !!这个操作是非阻塞的，Block仅代表能读出数据所需的时间，只有一条新消息也会立即返回；
// 若需要定时批量读取数据，"定时"需要自己用个time.Ticker
func (c *Consumer) ReadBatch(batchSize int64) ([]Message, error) {
	res, err := c.cli.XReadGroup(context.Background(), &redis.XReadGroupArgs{
		Group:    c.group,
		Streams:  []string{c.stream, ">"},
		Consumer: c.name,
		Block:    c.block,
		Count:    batchSize,
	}).Result()
	if err != nil && err != redis.Nil { // 阻塞超时返回(nil)
		return nil, err
	}
	if err == nil && len(res) > 0 {
		// []XStream面向不同的流，只从一个流读不用考虑；
		// 其实没必要拆这个包，直接用XMessage好了
		msgs := make([]Message, 0, len(res[0].Messages))
		for _, msg := range res[0].Messages {
			msgs = append(msgs, Message{
				ID:   msg.ID,
				Body: []byte(msg.Values["body"].(string)),
			})
		}
		return msgs, nil
	}

	// 阻塞超时，即等待Block时间后，仍旧没有收到任何消息
	resAC, start, err := c.cli.XAutoClaim(context.Background(), &redis.XAutoClaimArgs{
		Stream:   c.stream,
		Group:    c.group,
		Consumer: c.name,
		Count:    batchSize,
		MinIdle:  c.minIdle,
		Start:    c.start,
	}).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}
	c.start = start
	if err != nil || len(resAC) == 0 { // 真的没消息
		return nil, nil
	}
	// 拆包
	msgs := make([]Message, 0, len(resAC))
	for _, msg := range resAC {
		msgs = append(msgs, Message{
			ID:   msg.ID,
			Body: []byte(msg.Values["body"].(string)),
		})
	}
	return msgs, nil
}

func (c *Consumer) Ack(id ...string) (int64, error) {
	return c.cli.XAck(context.Background(), c.stream, c.group, id...).Result()
}

const ErrBusyGroup = "BUSYGROUP Consumer Group name already exists"

// 生产者/消费者初始化时调用
func GroupCreateMkStream(cli *redis.Client, stream, group string) error {
	err := cli.XGroupCreateMkStream(context.Background(), stream, group, "$").Err()
	if err != nil && err.Error() != ErrBusyGroup {
		return err
	}
	return nil
}
