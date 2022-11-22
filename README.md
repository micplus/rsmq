# RSMQ
 
Message Queue based on Redis Stream

生产者 -> Redis Stream -> 消费者

封装Redis Stream的基本操作，支持的功能：

1. 创建Stream、Group
2. 生产者向指定Stream逐条插入[]byte
3. 消费者从单个Stream，依照Group逐条或批量读取Message
4. 有超时机制，消费者阻塞读取一段时间后若没有读到新消息，则检查旧消息中是否有超时未被ACK的消息

Message定义如下

```go
type Message struct {
    ID string   // Redis Stream默认的"时间戳-顺序"ID
    Body []byte // 在Redis中表示为 "body": string
}
```

## dependency

- Go 1.19
- Redis v7.0.5
- github.com/go-redis/redis/v9

## 组成成分

1. 队列：Redis Stream
2. 生产者：向RS添加数据
3. 消费者：从RS读取数据

## 使用

### 生产者端

1. 启动Redis，初始化一个Redis Client
2. 初始化Stream（统一用GroupCreateMkStream，重复则忽略）
2. 传入Client和参数（队列长度限制），创建Producer
3. 使用Producer向指定的Stream逐条插入数据

### 消费者端

1. 启动Redis，初始化一个Redis Client
2. GroupCreateMkStream
3. 传入Client和各项参数，创建Consumer
4. 使用Consumer从单个Stream、Group逐条或批量读取数据
5. 处理完成后对应消息ID发出ACK信号，标记消息已被处理
