package fastrand

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"
)

// BenchSink prevents the compiler from optimizing away benchmark loops.
var BenchSink uint64

func BenchmarkUint64n(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		s := uint64(0)
		for pb.Next() {
			s += Uint64n(1e6)
		}
		atomic.AddUint64(&BenchSink, s)
	})
}

func BenchmarkInt64n(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		s := 0
		for pb.Next() {
			s += Intn(1e6)
		}
		atomic.AddUint64(&BenchSink, uint64(s))
	})
}

func BenchmarkRNGUint64n(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		var r RNG64
		s := uint64(0)
		for pb.Next() {
			s += r.Uint64n(1e6)
		}
		atomic.AddUint64(&BenchSink, s)
	})
}

func BenchmarkRNGUint64nWithLock(b *testing.B) {
	var r RNG64
	var rMu sync.Mutex
	b.RunParallel(func(pb *testing.PB) {
		s := uint64(0)
		for pb.Next() {
			rMu.Lock()
			s += r.Uint64n(1e6)
			rMu.Unlock()
		}
		atomic.AddUint64(&BenchSink, s)
	})
}

func BenchmarkRNGUint64nArray(b *testing.B) {
	var rr [64]struct {
		r  RNG64
		mu sync.Mutex

		// pad prevents from false sharing
		pad [64 - (unsafe.Sizeof(RNG64{})+unsafe.Sizeof(sync.Mutex{}))%64]byte
	}
	var n uint64
	b.RunParallel(func(pb *testing.PB) {
		s := uint64(0)
		for pb.Next() {
			idx := atomic.AddUint64(&n, 1)
			r := &rr[idx%uint64(len(rr))]
			r.mu.Lock()
			s += r.r.Uint64n(1e6)
			r.mu.Unlock()
		}
		atomic.AddUint64(&BenchSink, s)
	})
}

func BenchmarkMathRandInt63n(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		s := uint64(0)
		for pb.Next() {
			s += uint64(rand.Int63n(1e6))
		}
		atomic.AddUint64(&BenchSink, s)
	})
}

func BenchmarkMathRandRNGInt63n(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(42))
		s := uint64(0)
		for pb.Next() {
			s += uint64(r.Int63n(1e6))
		}
		atomic.AddUint64(&BenchSink, s)
	})
}

func BenchmarkMathRandRNGInt63nWithLock(b *testing.B) {
	r := rand.New(rand.NewSource(42))
	var rMu sync.Mutex
	b.RunParallel(func(pb *testing.PB) {
		s := uint64(0)
		for pb.Next() {
			rMu.Lock()
			s += uint64(r.Int63n(1e6))
			rMu.Unlock()
		}
		atomic.AddUint64(&BenchSink, s)
	})
}

func BenchmarkMathRandRNGInt63nArray(b *testing.B) {
	var rr [64]struct {
		r  *rand.Rand
		mu sync.Mutex

		// pad prevents from false sharing
		pad [64 - (unsafe.Sizeof(RNG64{})+unsafe.Sizeof(sync.Mutex{}))%64]byte
	}
	for i := range rr {
		rr[i].r = rand.New(rand.NewSource(int64(i)))
	}

	var n uint64
	b.RunParallel(func(pb *testing.PB) {
		s := uint64(0)
		for pb.Next() {
			idx := atomic.AddUint64(&n, 1)
			r := &rr[idx%uint64(len(rr))]
			r.mu.Lock()
			s += uint64(r.r.Int63n(1e6))
			r.mu.Unlock()
		}
		atomic.AddUint64(&BenchSink, s)
	})
}

func TestRand(t *testing.T) {
	for i := 0; i < 100; i++ {
		// fmt.Println(Uint64n(10))
		fmt.Println(Int63n(3))
	}
}