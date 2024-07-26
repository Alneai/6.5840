package raft

import (
	"log"
	"os"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		logger := log.New(os.Stdout, "", log.Ltime)
		logger.Printf(format, a...)
	}
}
