package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func main() {

	//flags / params
	syncPtr := flag.Int("sync", 1, "No Sync (0) | Sync on Write (1) | on Timer (2)")
	srcePtr := flag.String("bind", ":6380", "interfaz a escuchar")
	flag.Parse()

	if *syncPtr < 0 || *syncPtr > 2 {
		fmt.Printf("%v no es un modo soportado", *syncPtr)
		return
	}

	store, err := Open(".", SyncType(*syncPtr))
	if err != nil {
		log.Fatal(err)
	}

	srv := NewServer(store)

	//init
	ln, err := net.Listen("tcp", *srcePtr)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Ferdis up en %s\n", *srcePtr)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		stop()
		ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if errors.Is(err, net.ErrClosed) {
			break //shutting down
		}
		if err != nil {
			log.Println(err)
			continue
		}
		srv.track(conn)
		srv.wg.Add(1)
		go srv.handleClient(conn)
	}

	srv.closeAll()
	srv.wg.Wait()
	if err := store.Close(); err != nil {
		fmt.Printf("err %s\n", err)
	}
}

func Serve(store *Store, r io.Reader, w io.Writer) {
	scanner := bufio.NewScanner(r)

	for {
		fmt.Fprint(w, "> ")
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		parts := strings.SplitN(line, " ", 3)
		inptLen := len(parts)

		switch strings.ToLower(parts[0]) {
		case "get":
			if inptLen != 2 {
				fmt.Fprintln(w, "uso: get <key>")
				continue
			}
			val, ok := store.Get(parts[1])
			if ok {
				fmt.Fprintln(w, val)
			} else {
				fmt.Fprintln(w, "(nil)")
			}
		case "set":
			if inptLen != 3 {
				fmt.Fprintln(w, "uso: set <key> <val>")
				continue
			}
			err := store.Set(parts[1], parts[2])
			if err != nil {
				fmt.Fprintln(w, "error:", err)
			} else {
				fmt.Fprintln(w, "ok")
			}
		case "del":
			if inptLen != 2 {
				fmt.Fprintln(w, "uso: del <key>")
				continue
			}
			err := store.Delete(parts[1])
			if err != nil {
				fmt.Fprintln(w, "error:", err)
			} else {
				fmt.Fprintln(w, "ok")
			}
		case "exit":
			// if err := store.Compact(); err != nil {
			// 	fmt.Fprintln(w, "error:", err)
			// } else {
			fmt.Fprintln(w, "bye")
			// }
			return

		case "bench":
			if inptLen != 2 {
				fmt.Fprintln(w, "uso: bench <record amount>")
				continue
			}
			part1, err := strconv.Atoi(parts[1])
			if err != nil {
				fmt.Fprintln(w, "uso: bench <record amount>")
				continue
			}
			if part1 <= 0 {
				fmt.Fprintln(w, "uso: bench <record amount>")
				continue
			}

			start := time.Now()
			var setErr error
			for i := 0; i < part1; i++ {
				if setErr = store.Set(fmt.Sprintf("k%d", i), "value"); setErr != nil {
					fmt.Fprintln(w, "error:", setErr)
					break
				}
			}
			if setErr != nil {
				continue
			}
			fmt.Fprintf(w, "%d sets in %v (%v/op)\n", part1, time.Since(start), time.Since(start)/time.Duration(part1))

		case "compact":
			oLen, err := os.Stat(store.path)
			if err != nil {
				fmt.Fprintln(w, "error:", err)
				continue
			}
			if err := store.Compact(); err != nil {
				fmt.Fprintln(w, "error:", err)
				continue
			} else {
				nLen, err := os.Stat(store.path)
				if err != nil {
					fmt.Fprintln(w, "error:", err)
					continue
				}
				fmt.Fprintf(w, "compact ok: %v -> %v bytes\n", oLen.Size(), nLen.Size())
			}
		case "backup":
			if err := store.backup(); err != nil {
				fmt.Fprintln(w, "error:", err)
			} else {
				fmt.Fprintln(w, "backup done")
			}

		default:
			fmt.Fprintln(w, "uso: <comando> <key> <val>")
		}
	}
	if err := scanner.Err(); err != nil {
		log.Println("client error:", err)
	}
}
