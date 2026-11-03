package pool

import (
	"context"
	"fmt"
	"hash/crc32"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// stackFrame takes about 512 bytes of stack per call.
//
//go:noinline
func stackFrame(depth int, sink *[64]uint64) uint64 {
	var buf [64]uint64
	buf[depth%64] = uint64(depth)
	if depth == 0 {
		*sink = buf
		return buf[0]
	}
	return stackFrame(depth-1, &buf) + buf[depth%64]
}

var benchSink atomic.Uint64

// newBenchFactory creates a factory with shards that are only used for key
// distribution; fn in the benchmarks never touches connections.
func newBenchFactory(shards, workers int) *ShardConnFactory {
	factory := &ShardConnFactory{
		cfg: &ShardConfig{
			DistributeType:  DistributeByModular,
			HashFn:          crc32.ChecksumIEEE,
			MultiKeyWorkers: workers,
		},
		shards: make([]*HAConnFactory, shards),
	}
	factory.startWorkers()
	return factory
}

// BenchmarkDoMultiKeys measures the overhead of splitting a multi-key command
// by shards. fn imitates a call chain that uses stackKB of stack, like the
// go-redis hooks + TLS write chain on a real MGET.
func BenchmarkDoMultiKeys(b *testing.B) {
	const shards = 16

	// Like in production, most goroutines sleep with a small stack. They keep
	// the runtime's average stack size low, so new goroutines start small.
	stop := make(chan struct{})
	defer close(stop)
	for i := 0; i < 130; i++ {
		go func() { <-stop }()
	}

	keys := make([]string, 100)
	for i := range keys {
		keys[i] = fmt.Sprintf("key:%d", 1000000+i*7919)
	}

	for _, stackKB := range []int{2, 8, 16} {
		depth := stackKB * 2 // 512 B per frame
		fn := func(_ *ShardConnFactory, keyList ...string) redis.Cmder {
			var sink [64]uint64
			benchSink.Add(stackFrame(depth, &sink))
			return redis.NewSliceCmd(context.Background(), "mget", keyList[0])
		}

		for _, impl := range []struct {
			name    string
			workers int
		}{
			{"goroutine", -1},
			{"pool", 1024},
		} {
			b.Run(fmt.Sprintf("stack=%dKB/impl=%s", stackKB, impl.name), func(b *testing.B) {
				factory := newBenchFactory(shards, impl.workers)
				want := len(factory.groupKeysByInd(keys...))

				b.ReportAllocs()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						if res := factory.doMultiKeys(fn, keys...); len(res) != want {
							b.Fatalf("got %d results", len(res))
						}
					}
				})
			})
		}
	}
}

// BenchmarkDoMultiKeysAfterBurst imitates a low load after a traffic spike:
// a burst starts many workers, then commands go one at a time with GC cycles
// between them. Workers that wait long between tasks get their stacks shrunk
// by the GC and grow them again on the next task.
func BenchmarkDoMultiKeysAfterBurst(b *testing.B) {
	const (
		shards  = 16
		stackKB = 16
	)
	stop := make(chan struct{})
	defer close(stop)
	for i := 0; i < 130; i++ {
		go func() { <-stop }()
	}

	keys := make([]string, 100)
	for i := range keys {
		keys[i] = fmt.Sprintf("key:%d", 1000000+i*7919)
	}
	fn := func(_ *ShardConnFactory, keyList ...string) redis.Cmder {
		var sink [64]uint64
		benchSink.Add(stackFrame(stackKB*2, &sink))
		return redis.NewSliceCmd(context.Background(), "mget", keyList[0])
	}

	for _, gcEvery := range []int{4, 32} {
		for _, impl := range []struct {
			name    string
			workers int
		}{
			{"goroutine", -1},
			{"pool", 1024},
		} {
			b.Run(fmt.Sprintf("gcEvery=%d/impl=%s", gcEvery, impl.name), func(b *testing.B) {
				// In production a load lasts hours, much longer than the idle
				// timeout. A short timeout keeps the same ratio in a benchmark.
				factory := newBenchFactory(shards, impl.workers)
				factory.cfg.MultiKeyWorkerIdleTimeout = 50 * time.Millisecond

				// The burst: 64 concurrent commands start up to 64 workers per shard.
				var wg sync.WaitGroup
				start := make(chan struct{})
				for g := 0; g < 64; g++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						<-start
						for i := 0; i < 20; i++ {
							factory.doMultiKeys(fn, keys...)
						}
					}()
				}
				close(start)
				wg.Wait()

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if i%gcEvery == 0 {
						runtime.GC()
					}
					factory.doMultiKeys(fn, keys...)
				}
				b.StopTimer()
				b.ReportMetric(float64(factory.workers.Load()), "workers")
			})
		}
	}
}
