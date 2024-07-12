package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"
)

type RedisServer struct {
	data map[string]string
	mu   sync.RWMutex
}

func NewRedisServer() *RedisServer {
	return &RedisServer{
		data: make(map[string]string),
	}
}

func (s *RedisServer) handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	conn.Write([]byte("Welcome to the Redis server!\r\n"))
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimSpace(line)
		args := strings.Split(line, " ")
		if len(args) == 0 {
			continue
		}
		command := strings.ToUpper(args[0])
		switch command {
		case "SET":
			if len(args) != 3 {
				conn.Write([]byte("ERR wrong number of arguments for 'set' command\r\n"))
				continue
			}
			s.mu.Lock()
			s.data[args[1]] = args[2]
			s.mu.Unlock()
			conn.Write([]byte("OK\r\n"))
		case "GET":
			if len(args) != 2 {
				conn.Write([]byte("ERR wrong number of arguments for 'get' command\r\n"))
				continue
			}
			s.mu.RLock()
			value, ok := s.data[args[1]]
			s.mu.RUnlock()
			if ok {
				conn.Write([]byte(value + "\r\n"))
			} else {
				conn.Write([]byte("nil\r\n"))
			}
		case "PING":
			conn.Write([]byte("PONG\r\n"))
		default:
			conn.Write([]byte(fmt.Sprintf("ERR unknown command '%s'\r\n", command)))
		}
	}
}

func (s *RedisServer) Run(port string) {
	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Redis server is running on port %s\n", port)
	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		conn.Write([]byte("init handleConnection\r\n"))
		go s.handleConnection(conn)
	}
}

func main() {
	server := NewRedisServer()
	server.Run("6379")
}
