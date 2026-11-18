package main

import (
	"net"
	"sync"
)

type Server struct {
	store *Store
	wg    sync.WaitGroup
	mu    sync.Mutex
	conns map[net.Conn]struct{}
}

func NewServer(store *Store) *Server {
	return &Server{conns: make(map[net.Conn]struct{}), store: store}
}

func (srv *Server) handleClient(conn net.Conn) {
	defer srv.wg.Done()
	defer srv.untrack(conn)
	defer conn.Close()

	Serve(srv.store, conn, conn)
}

func (srv *Server) track(c net.Conn) {
	srv.mu.Lock()
	srv.conns[c] = struct{}{}
	srv.mu.Unlock()
}

func (srv *Server) untrack(c net.Conn) {
	srv.mu.Lock()
	delete(srv.conns, c)
	srv.mu.Unlock()
}

func (srv *Server) closeAll() {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	for c := range srv.conns {
		c.Close()
	}
}
