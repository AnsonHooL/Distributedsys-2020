package raft

import "log"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const KVDebug = 0

func KVPrintf(format string, a ...interface{}) (n int, err error) {
	if KVDebug > 0 {
		log.Printf(format, a...)
	}
	return
}