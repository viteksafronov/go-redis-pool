package pool

import (
	"context"
	"fmt"
	"hash/crc32"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func newWorkersFactory(shards, workers int, idle time.Duration) *ShardConnFactory {
	factory := &ShardConnFactory{
		cfg: &ShardConfig{
			DistributeType:            DistributeByModular,
			HashFn:                    crc32.ChecksumIEEE,
			MultiKeyWorkers:           workers,
			MultiKeyWorkerIdleTimeout: idle,
		},
		shards: make([]*HAConnFactory, shards),
	}
	factory.startWorkers()
	return factory
}

// echoFn returns the keys it got, so results can be checked against the input.
func echoFn(_ *ShardConnFactory, keyList ...string) redis.Cmder {
	cmd := redis.NewStringSliceCmd(context.Background())
	cmd.SetVal(append([]string(nil), keyList...))
	return cmd
}

func checkResults(t *testing.T, factory *ShardConnFactory, keys []string) {
	t.Helper()

	results := factory.doMultiKeys(echoFn, keys...)
	if want := len(factory.groupKeysByInd(keys...)); len(results) != want {
		t.Fatalf("got %d results, want %d", len(results), want)
	}

	var got []string
	for _, res := range results {
		keyList := res.(*redis.StringSliceCmd).Val()
		ind := factory.getShardIndex(keyList[0])
		for _, key := range keyList {
			if factory.getShardIndex(key) != ind {
				t.Fatalf("key %s is mixed into shard %d", key, ind)
			}
		}
		got = append(got, keyList...)
	}

	sort.Strings(got)
	want := append([]string(nil), keys...)
	sort.Strings(want)
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got keys %v, want %v", got, want)
	}
}

func testKeys(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("key:%d", i)
	}
	return keys
}

func TestDoMultiKeysWorkers(t *testing.T) {
	for _, workers := range []int{-1, 1, 4, 1024} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			factory := newWorkersFactory(16, workers, time.Minute)
			keys := testKeys(100)

			var wg sync.WaitGroup
			for g := 0; g < 32; g++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < 200; i++ {
						checkResults(t, factory, keys)
					}
				}()
			}
			wg.Wait()

			limit := int32(workers)
			if limit < 0 {
				limit = 0
			}
			if n := factory.workers.Load(); n > limit {
				t.Fatalf("%d workers are running, limit is %d", n, limit)
			}
		})
	}
}

func TestDoMultiKeysWorkersExitWhenIdle(t *testing.T) {
	factory := newWorkersFactory(16, 64, 50*time.Millisecond)
	checkResults(t, factory, testKeys(100))

	if factory.workers.Load() == 0 {
		t.Fatal("no workers were started")
	}

	deadline := time.Now().Add(5 * time.Second)
	for factory.workers.Load() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%d workers are still running", factory.workers.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}

	// The factory keeps working after all workers have exited.
	checkResults(t, factory, testKeys(100))
}

func TestMultiKeyWorkersConfig(t *testing.T) {
	for _, workers := range []int{0, -1} {
		if f := newWorkersFactory(16, workers, 0); f.tasks != nil {
			t.Fatalf("workers=%d must disable the workers", workers)
		}
	}

	f := newWorkersFactory(16, 4, 0)
	if f.tasks == nil {
		t.Fatal("workers=4 must enable the workers")
	}
	if f.cfg.MultiKeyWorkerIdleTimeout != DefaultMultiKeyWorkerIdleTimeout {
		t.Fatalf("idle timeout %v, want default %v", f.cfg.MultiKeyWorkerIdleTimeout, DefaultMultiKeyWorkerIdleTimeout)
	}
}
