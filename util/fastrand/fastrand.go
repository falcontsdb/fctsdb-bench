// Package fastrand implements fast pesudorandom number generator
// that should scale well on multi-CPU systems.
//
// Use crypto/rand instead of this package for generating
// cryptographically secure random numbers.
package fastrand

import (
	"sync"
	"time"
	_ "unsafe"
)

const (
	rngMax  = 1 << 63
	rngMask = rngMax - 1
)

// Uint32 returns a lock free uint32 value.
//go:linkname Uint32 runtime.fastrand
func Uint32() uint32

// Uint32n returns a lock free uint32 value in the interval [0, n).
//go:linkname Uint32n runtime.fastrandn
func Uint32n(n uint32) uint32

// Uint32 returns pseudorandom uint32.
//
// It is safe calling this function from concurrent goroutines.
func Uint64() uint64 {
	v := rngPool.Get()
	if v == nil {
		v = &RNG64{}
	}
	r := v.(*RNG64)
	x := r.Uint64()
	rngPool.Put(r)
	return x
}

func Int63() int64 {
	v := rngPool.Get()
	if v == nil {
		v = &RNG64{}
	}
	r := v.(*RNG64)
	x := r.Int63()
	rngPool.Put(r)
	return x
}

var rngPool sync.Pool

// Uint32n returns pseudorandom uint32 in the range [0..maxN).
//
// It is safe calling this function from concurrent goroutines.
func Uint64n(maxN uint64) uint64 {
	x := Uint64()
	// See http://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
	return uint64((uint64(x) * uint64(maxN)) >> 32)
}

func Intn(n int) int {
	return int(Int63() % int64(n))
}

func Int63n(n int64) int64 {
	return Int63() % n
}

// RNG64 is a pseudorandom number generator.
//
// It is unsafe to call RNG64 methods from concurrent goroutines.
type RNG64 struct {
	x uint64
}

func (r *RNG64) Int63() int64 {
	return int64(r.Uint64() & rngMask)
}

// Uint32 returns pseudorandom uint32.
//
// It is unsafe to call this method from concurrent goroutines.
func (r *RNG64) Uint64() uint64 {
	for r.x == 0 {
		r.x = getRandomUint64()
	}

	// See https://en.wikipedia.org/wiki/Xorshift
	x := r.x
	x ^= x << 13
	x ^= x >> 7
	x ^= x << 17
	r.x = x
	return x
}

// Uint32n returns pseudorandom uint32 in the range [0..maxN).
//
// It is unsafe to call this method from concurrent goroutines.
func (r *RNG64) Uint64n(maxN uint64) uint64 {
	x := r.Uint64()
	// See http://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
	return uint64((uint64(x) * uint64(maxN)) >> 32)
}

// Seed sets the r state to n.
func (r *RNG64) Seed(n uint64) {
	r.x = n
}

func getRandomUint64() uint64 {
	x := time.Now().UnixNano()
	return uint64((x >> 32) ^ x)
}
