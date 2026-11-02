package pool

import (
	"errors"
	"hash/crc32"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bitleak/go-redis-pool/v3/hashkit"
	"github.com/redis/go-redis/v9"
)

// DefaultMultiKeyWorkerIdleTimeout is the default ShardConfig.MultiKeyWorkerIdleTimeout.
const DefaultMultiKeyWorkerIdleTimeout = 10 * time.Second

const (
	// DistributeByModular selects the sharding factory by modular
	DistributeByModular = iota + 1
	// DistributeByKetama selects the sharding factory by ketama consistent algorithm
	DistributeByKetama
)

var (
	errMoreThanOneParam = errors.New("the number of params shouldn't be greater than 1")
)

type ShardConfig struct {
	Shards         []*HAConfig
	DistributeType int // distribution type of the shards, supports `DistributeByModular` or `DistributeByKetama`
	HashFn         func(key []byte) uint32
	// MultiKeyWorkers is the maximum number of long-lived goroutines that run
	// per-shard parts of multi-key commands (MGET, DEL, ...). By default every
	// part runs in a new goroutine. A new goroutine starts with a small stack,
	// and a deep call chain (hooks, TLS) makes the runtime grow and copy it on
	// every command. A worker's stack is grown once and reused.
	//
	// Workers are started on demand and exit after MultiKeyWorkerIdleTimeout
	// without tasks, also after the pool is closed. When all workers are busy
	// and the limit is reached, a one-off goroutine is started as before.
	// Zero or a negative value disables the workers.
	MultiKeyWorkers int
	// MultiKeyWorkerIdleTimeout is how long an idle worker waits for a task
	// before it exits. Defaults to DefaultMultiKeyWorkerIdleTimeout.
	MultiKeyWorkerIdleTimeout time.Duration
}

type ShardConnFactory struct {
	cfg    *ShardConfig
	shards []*HAConnFactory
	hash   hashkit.HashKit

	tasks   []chan multiKeyTask // per shard
	workers atomic.Int32
}

// multiKeyTask is a per-shard part of a multi-key command.
type multiKeyTask struct {
	fn      multiKeyFn
	keys    []string
	results []redis.Cmder
	i       int
	wg      *sync.WaitGroup
}

func (t *multiKeyTask) run(factory *ShardConnFactory) {
	t.results[t.i] = t.fn(factory, t.keys...)
	t.wg.Done()
}

func NewShardConnFactory(cfg *ShardConfig) (*ShardConnFactory, error) {
	factory := &ShardConnFactory{
		cfg:    cfg,
		shards: make([]*HAConnFactory, len(cfg.Shards)),
	}
	if factory.cfg.DistributeType < DistributeByModular || factory.cfg.DistributeType > DistributeByKetama {
		cfg.DistributeType = DistributeByModular
	}
	var err error
	for i, shard := range cfg.Shards {
		if factory.shards[i], err = NewHAConnFactory(shard); err != nil {
			return nil, err
		}
	}
	if factory.cfg.HashFn == nil {
		factory.cfg.HashFn = crc32.ChecksumIEEE
	}
	if cfg.DistributeType == DistributeByKetama {
		servers := make([]*hashkit.Server, 0)
		for idx, shard := range factory.shards {
			servers = append(servers, &hashkit.Server{
				Name:   shard.cfg.Master,
				Weight: 1,
				Index:  uint32(idx),
			})
		}
		factory.hash = hashkit.NewKetama(servers, factory.cfg.HashFn)
	}
	factory.startWorkers()
	return factory, nil
}

func (factory *ShardConnFactory) startWorkers() {
	if factory.cfg.MultiKeyWorkers <= 0 {
		return
	}
	if factory.cfg.MultiKeyWorkerIdleTimeout <= 0 {
		factory.cfg.MultiKeyWorkerIdleTimeout = DefaultMultiKeyWorkerIdleTimeout
	}
	// A channel per shard spreads contention between concurrent commands.
	// Channels are unbuffered: a task is handed over only to an idle worker.
	factory.tasks = make([]chan multiKeyTask, len(factory.shards))
	for i := range factory.tasks {
		factory.tasks[i] = make(chan multiKeyTask)
	}
}

// runTask hands the task over to an idle worker, starts a new worker if the
// limit allows, or falls back to a one-off goroutine.
func (factory *ShardConnFactory) runTask(ind uint32, task multiKeyTask) {
	if factory.tasks == nil {
		go task.run(factory)
		return
	}
	tasks := factory.tasks[ind]
	select {
	case tasks <- task:
		return
	default:
	}
	if factory.workers.Add(1) <= int32(factory.cfg.MultiKeyWorkers) {
		go factory.worker(tasks, task)
		return
	}
	factory.workers.Add(-1)
	go task.run(factory)
}

func (factory *ShardConnFactory) worker(tasks chan multiKeyTask, task multiKeyTask) {
	defer factory.workers.Add(-1)

	task.run(factory)

	idle := factory.cfg.MultiKeyWorkerIdleTimeout
	timer := time.NewTimer(idle)
	defer timer.Stop()
	for {
		select {
		case task := <-tasks:
			task.run(factory)
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(idle)
		case <-timer.C:
			// Workers have no stop channel: a select on a channel shared by all
			// workers would contend on its lock. After close they exit by timeout.
			return
		}
	}
}

func (factory *ShardConnFactory) stats() map[string]*redis.PoolStats {
	results := make(map[string]*redis.PoolStats, len(factory.shards))
	for _, shard := range factory.shards {
		result := shard.stats()
		for addr, stats := range result {
			results[addr] = stats
		}
	}
	return results
}

func (factory *ShardConnFactory) close() {
	for _, shard := range factory.shards {
		shard.close()
	}
}

func (factory *ShardConnFactory) getShardIndex(key string) uint32 {
	key = extractHashPrefix(key)
	switch factory.cfg.DistributeType {
	case DistributeByKetama:
		return factory.hash.Dispatch(key)
	default:
		return factory.cfg.HashFn([]byte(key)) % uint32(len(factory.shards))
	}
}

func (factory *ShardConnFactory) getSlaveConn(key ...string) (*redis.Client, error) {
	if len(key) > 1 {
		return nil, errMoreThanOneParam
	}
	var ind uint32
	ind = 0
	if len(key) > 0 {
		ind = factory.getShardIndex(key[0])
	}
	return factory.shards[ind].getSlaveConn()
}

func (factory *ShardConnFactory) getMasterConn(key ...string) (*redis.Client, error) {
	if len(key) > 1 {
		return nil, errMoreThanOneParam
	}
	var ind uint32
	ind = 0
	if len(key) > 0 {
		ind = factory.getShardIndex(key[0])
	}
	return factory.shards[ind].getMasterConn()
}

func (factory *ShardConnFactory) groupKeysByInd(keys ...string) map[uint32][]string {
	index2Keys := make(map[uint32][]string, 0)
	for i := 0; i < len(keys); i++ {
		ind := factory.getShardIndex(keys[i])
		if _, exists := index2Keys[ind]; !exists {
			index2Keys[ind] = make([]string, 0)
		}
		index2Keys[ind] = append(index2Keys[ind], keys[i])
	}
	return index2Keys
}

func (factory *ShardConnFactory) isCrossMultiShards(keys ...string) bool {
	var ind uint32
	for i := 0; i < len(keys); i++ {
		newInd := factory.getShardIndex(keys[i])
		if i == 0 {
			ind = newInd
		} else if newInd != ind {
			return true
		}
	}
	return false
}

type multiKeyFn func(factory *ShardConnFactory, keys ...string) redis.Cmder

func (factory *ShardConnFactory) doMultiKeys(fn multiKeyFn, keys ...string) []redis.Cmder {
	if len(keys) == 1 {
		return []redis.Cmder{fn(factory, keys...)}
	}
	index2Keys := factory.groupKeysByInd(keys...)
	if len(index2Keys) == 1 {
		return []redis.Cmder{fn(factory, keys...)}
	}

	var wg sync.WaitGroup
	results := make([]redis.Cmder, len(index2Keys))
	var i int
	for ind, keyList := range index2Keys {
		wg.Add(1)
		factory.runTask(ind, multiKeyTask{fn: fn, keys: keyList, results: results, i: i, wg: &wg})
		i++
	}
	wg.Wait()
	return results
}

func (factory *ShardConnFactory) doMultiIntCommand(fn multiKeyFn, keys ...string) (int64, error) {
	var err error
	total := int64(0)
	results := factory.doMultiKeys(fn, keys...)
	for _, result := range results {
		cmd := result.(*redis.IntCmd)
		if cmd.Err() != nil {
			err = cmd.Err()
			continue
		}
		total += cmd.Val()
	}
	return total, err
}
