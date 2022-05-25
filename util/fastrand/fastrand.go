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

// Uint32n 在gccgo时不能编译
// //go:linkname Uint32n runtime.fastrandn
// func Uint32n(n uint32) uint32

func Uint32n(n uint32) uint32 {
	return uint32(uint64(Uint32()) * uint64(n) >> 32)
}

func Uint64() uint64 {
	return uint64(Uint32())<<32 | uint64(Uint32())
}

func Uint64n(n uint64) uint64 {
	return Uint64() % n
}

func Int64() int64 {
	return int64(Uint32())<<32 | int64(Uint32())
}

func Int63() int64 {
	return int64(Uint32())<<31 | int64(Uint32())
}

func Float64() float64 {
again:
	f := float64(Int63()) / (1 << 63)
	if f == 1 {
		goto again // resample; this branch is taken O(never)
	}
	return f
}

func RandomNormalBytes(n int) []byte {
	letterBytes := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	letterIdxBits := 6                            // 6 bits to represent a letter index
	letterIdxMask := uint32(1<<letterIdxBits - 1) // All 1-bits, as many as letterIdxBits
	letterIdxMax := 32 / letterIdxBits            // # of letter indices fitting in 63 bits

	// sb := strings.Builder{}
	// sb.Grow(n)
	buf := make([]byte, n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, Uint32(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = Uint32(), letterIdxMax
		}
		idx := int(cache&letterIdxMask) % len(letterBytes)
		buf[i] = letterBytes[idx]
		i--

		cache >>= letterIdxBits
		remain--
	}

	// return *(*string)(unsafe.Pointer(&buf))
	return buf
}
