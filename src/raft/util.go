package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = true

const (
	HeartbeatInterval    = time.Duration(120) * time.Millisecond
	ElectionTimeoutLower = time.Duration(300) * time.Millisecond
	ElectionTimeoutWave  = time.Duration(100) * time.Millisecond
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func RandomizedElectionTimeout() time.Duration {
	num := rand.Int63n(ElectionTimeoutWave.Nanoseconds()) + ElectionTimeoutLower.Nanoseconds()
	return time.Duration(num) * time.Nanosecond
}

func StableHeartbeatTimeout() time.Duration {
	return HeartbeatInterval
}

//利用一个select来包裹channel drain，这样无论channel中是否有数据，drain都不会阻塞住
func resetTimer(timer *time.Timer, d time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C: //try to drain from the channel
		default:
		}
	}
	timer.Reset(d)
}

func Max(x, y int) int {
	if x > y {
		return x
	} else {
		return y
	}
}

func Min(x, y int) int {
	if x < y {
		return x
	} else {
		return y
	}
}
