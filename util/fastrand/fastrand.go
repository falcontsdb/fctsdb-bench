// Package fastrand implements fast pesudorandom number generator
// that should scale well on multi-CPU systems.
//
// Use crypto/rand instead of this package for generating
// cryptographically secure random numbers.
package fastrand

import (
	_ "unsafe"
)

// fastrand 提供的所有uint类型都是更快且安全的的伪随机值，但是和指定seed无关连

const (
	rngMax  = 1 << 63
	rngMask = rngMax - 1
)

// 这里是引用runtime.fastrand，协程安全，但是不能指定seed
// Uint32 returns a lock free uint32 value.
//go:linkname Uint32 runtime.fastrand
func Uint32() uint32

// 这里是引用runtime.fastrandn，协程安全，但是不能指定seed
// Uint32n returns a lock free uint32 value in the interval [0, n).
//go:linkname Uint32n runtime.fastrandn
func Uint32n(n uint32) uint32

func Uint64() uint64 {
	return uint64(Uint32())<<32 | uint64(Uint32())
}

func Uint64n(n uint64) uint64 {
	return Uint64() % n
}
