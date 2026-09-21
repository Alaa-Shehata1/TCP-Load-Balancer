package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
)

func main() {
	id := flag.String("id", "A", "server id")
	port := flag.String("port", "9001", "listen port")
	flag.Parse()
	ln, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Printf("echo %s on :%s", *id, *port)
	for {
		c, err := ln.Accept()
		if err != nil {
			continue
		}
		go func(conn net.Conn) {
			defer func() { _ = conn.Close() }()
			_, _ = fmt.Fprintf(conn, "served-by:%s\n", *id)
			_, _ = io.Copy(conn, conn)
		}(c)
	}
}
