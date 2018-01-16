package ring

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"stathat.com/c/consistent"

	"github.com/garyburd/redigo/redis"
)

// RingOptions are used to configure a ring client and should be
// passed to NewRing.
type RingOptions struct {
	Addrs []string

	// Frequency of PING commands sent to check shards availability.
	// Shard is considered down after 3 subsequent failed checks.
	HeartbeatFrequency time.Duration
	Threshold          int32

	DB       int
	Password string

	MaxIdle     int
	MaxActive   int
	IdleTimeout time.Duration
}

func (opt *RingOptions) init() {
	if opt.HeartbeatFrequency == 0 {
		opt.HeartbeatFrequency = 1 * time.Second
	}

	if opt.Threshold <= 0 {
		opt.Threshold = 3
	}

	if opt.MaxActive == 0 {
		opt.MaxActive = 500
	}

	if opt.MaxIdle == 0 {
		opt.MaxIdle = 50
	}

	if opt.IdleTimeout == 0 {
		opt.IdleTimeout = 60 * time.Second
	}
}

// Ring 结构体
type Ring struct {
	// 读写锁
	mu sync.RWMutex
	// 一致性哈希
	hash *consistent.Consistent
	// node -> *ringShard
	shards map[string]*ringShard
	// shardList: 保留注册节点顺序
	shardList []*ringShard

	opt *RingOptions

	closed bool
}

// NewRing 创建ring实例
func NewRing(opt *RingOptions) *Ring {
	opt.init()

	hash := consistent.New()
	hash.Set(opt.Addrs)

	r := &Ring{
		opt:    opt,
		hash:   hash,
		shards: make(map[string]*ringShard),
	}

	for _, node := range opt.Addrs {
		shard := &ringShard{
			node:      node,
			client:    r.getRedisPool(node),
			threshold: opt.Threshold,
		}
		r.shardList = append(r.shardList, shard)
		r.shards[node] = shard
	}

	go r.heartbeat()

	return r
}

// Pick 根据key选择hashring中的redis节点
func (r *Ring) Pick(key string) *redis.Pool {

	r.mu.RLock()
	defer r.mu.RUnlock()

	// 若redis都挂了或被Close，随机返回一个Pool避免panic
	if r.closed {
		return r.randomPick()
	}

	node, err := r.hash.Get(key)
	if err != nil {
		log.Printf("not found key(%s) err(%s)", key, err)
		return r.randomPick()
	}

	return r.shards[node].client
}

func (r *Ring) randomPick() *redis.Pool {
	for _, v := range r.shards {
		return v.client
	}

	log.Println("randomPick should never come here")
	return nil
}

type ringShard struct {
	client    *redis.Pool
	node      string
	down      int32
	threshold int32
}

func (shard *ringShard) String() string {
	var state string
	if shard.IsUp() {
		state = "up"
	} else {
		state = "down"
	}
	return fmt.Sprintf("%s is %s", shard.node, state)
}

func (shard *ringShard) IsDown() bool {
	return atomic.LoadInt32(&shard.down) >= shard.threshold
}

func (shard *ringShard) IsUp() bool {
	return !shard.IsDown()
}

// Vote votes to set shard state and returns true if state was changed.
func (shard *ringShard) Vote(up bool) bool {
	if up {
		changed := shard.IsDown()
		atomic.StoreInt32(&shard.down, 0)
		return changed
	}

	if shard.IsDown() {
		return false
	}

	atomic.AddInt32(&shard.down, 1)
	return shard.IsDown()
}

func (r *Ring) getRedisPool(node string) *redis.Pool {
	pool := &redis.Pool{
		MaxIdle:     r.opt.MaxIdle,
		MaxActive:   r.opt.MaxActive,
		IdleTimeout: r.opt.IdleTimeout,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", node)
			if err != nil {
				log.Printf("redis.Dial node(%s) failed", node)
				return nil, err
			}
			if len(r.opt.Password) > 0 {
				_, err = c.Do("AUTH", r.opt.Password)
				if err != nil {
					log.Printf("redis.Auth node(%s) failed", node)
					return nil, err
				}
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}

	return pool
}

// rebalance 重新生成hashring
func (r *Ring) rebalance() {
	hash := consistent.New()
	for _, shard := range r.shardList {
		if shard.IsUp() {
			hash.Add(shard.node)
		}
	}

	r.mu.Lock()
	r.hash = hash
	r.mu.Unlock()
	log.Printf("members: %s", r.hash.Members())
}

// heartbeat monitors state of each shard in the hash.
func (r *Ring) heartbeat() {
	ticker := time.NewTicker(r.opt.HeartbeatFrequency)
	defer ticker.Stop()

	for range ticker.C {
		var rebalance bool

		r.mu.RLock()

		if r.closed {
			r.mu.RUnlock()
			break
		}

		shards := r.shardList
		r.mu.RUnlock()

		for _, shard := range shards {
			conn := shard.client.Get()
			_, err := conn.Do("PING")
			if shard.Vote(err == nil) {
				rebalance = true
			}
			conn.Close()
		}

		if rebalance {
			log.Printf("redis ring state changed, trigger rebalance")
			r.rebalance()
		}
	}
}

// Close 关闭ring环中的redis
func (r *Ring) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var firstErr error
	for _, shard := range r.shardList {
		if err := shard.client.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	r.hash = nil
	r.shards = nil
	r.shardList = nil

	return firstErr
}
