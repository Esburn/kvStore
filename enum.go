package main

type SyncType int

const (
	SyncOnNone SyncType = iota
	SyncOnWrite
	SyncOnTimer
)
