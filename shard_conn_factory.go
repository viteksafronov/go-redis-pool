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

	idle    []idleWorkers // per shard
	workers atomic.Int32
}

// idleWorkers is a LIFO stack of the idle workers of a shard. A task goes to
// the most recently freed worker, so a small set of workers with grown stacks
// does all the work. Workers at the bottom stay idle and exit by timeout. With
// a FIFO queue tasks would rotate over all workers: none of them would reach
// the idle timeout, and the GC would shrink their stacks between tasks.
type idleWorkers struct {
	mu      sync.Mutex
	workers []*multiKeyWorker
}

type multiKeyWorker struct {
	tasks chan multiKeyTask // buffered: a sender never waits for the worker
}

// pop takes the most recently freed worker.
func (s *idleWorkers) pop() *multiKeyWorker {
	s.mu.Lock()
	n := len(s.workers)
	if n == 0 {
		s.mu.Unlock()
		return nil
	}
	w := s.workers[n-1]
	s.workers[n-1] = nil
	s.workers = s.workers[:n-1]
	s.mu.Unlock()
	return w
}

func (s *idleWorkers) push(w *multiKeyWorker) {
	s.mu.Lock()
	s.workers = append(s.workers, w)
	s.mu.Unlock()
}

// remove takes w out of the stack. It returns false if w has already been
// popped, then a task is being sent to it.
func (s *idleWorkers) remove(w *multiKeyWorker) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Workers that exit by timeout are at the bottom of the stack.
	for i, v := range s.workers {
		if v == w {
			copy(s.workers[i:], s.workers[i+1:])
			s.workers[len(s.workers)-1] = nil
			s.workers = s.workers[:len(s.workers)-1]
			return true
		}
	}
	return false
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
	// A stack per shard spreads contention between concurrent commands.
	factory.idle = make([]idleWorkers, len(factory.shards))
}

// runTask hands the task over to an idle worker, starts a new worker if the
// limit allows, or falls back to a one-off goroutine.
func (factory *ShardConnFactory) runTask(ind uint32, task multiKeyTask) {
	if factory.idle == nil {
		go task.run(factory)
		return
	}
	idle := &factory.idle[ind]
	if w := idle.pop(); w != nil {
		w.tasks <- task
		return
	}
	if factory.workers.Add(1) <= int32(factory.cfg.MultiKeyWorkers) {
		go factory.worker(idle, task)
		return
	}
	factory.workers.Add(-1)
	go task.run(factory)
}

func (factory *ShardConnFactory) worker(idle *idleWorkers, task multiKeyTask) {
	defer factory.workers.Add(-1)

	w := &multiKeyWorker{tasks: make(chan multiKeyTask, 1)}
	w.run(factory, idle, task)

	timeout := factory.cfg.MultiKeyWorkerIdleTimeout
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case task := <-w.tasks:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			w.run(factory, idle, task)
		case <-timer.C:
			// Workers have no stop channel: they exit by timeout, also after
			// the pool is closed.
			if idle.remove(w) {
				return
			}
			// The worker has just been popped, a task is on its way.
			w.run(factory, idle, <-w.tasks)
		}
		timer.Reset(timeout)
	}
}

// run executes the task and puts the worker back to the stack before the
// command is told that the part is done. So the next command of a caller finds
// the worker idle and does not start a new one.
func (w *multiKeyWorker) run(factory *ShardConnFactory, idle *idleWorkers, task multiKeyTask) {
	task.results[task.i] = task.fn(factory, task.keys...)
	idle.push(w)
	task.wg.Done()
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
