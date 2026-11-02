package pool

import (
	"context"
	"fmt"
	"hash/crc32"
	"sync/atomic"
	"testing"

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
		keys[i] = fmt.Sprintf("item:promotions:%d", 1000000+i*7919)
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
