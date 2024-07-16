package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type RedisServer struct {
	host             string
	port             int
	password         string
	db               int
	data             map[string]string
	expires          map[string]time.Time
	mu               sync.RWMutex
	aofFile          string
	aofBuffer        []string
	aofMu            sync.Mutex
	rdbFile          string
	rdbTempFile      string
	rdbSavingEnabled bool
}

func NewRedisServer(host string, port int, password string, db int, aofFile string, rdbFile string) *RedisServer {
	return &RedisServer{
		host:             host,
		port:             port,
		password:         password,
		db:               db,
		data:             make(map[string]string),
		expires:          make(map[string]time.Time),
		aofFile:          aofFile,
		aofBuffer:        make([]string, 0),
		rdbFile:          rdbFile,
		rdbTempFile:      rdbFile + ".temp",
		rdbSavingEnabled: true,
	}
}

func (s *RedisServer) handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				log.Printf("Client disconnected")
			} else {
				log.Printf("Error reading from client: %v", err)
			}
			return
		}
		line = strings.TrimSpace(line)
		args := strings.Split(line, " ")
		if len(args) == 0 {
			continue
		}
		command := strings.ToUpper(args[0])
		response := s.handleCommand(command, args[1:]...)
		writer.WriteString(response + "\r\n")
		writer.Flush()
	}
}

func (s *RedisServer) handleCommand(command string, args ...string) string {
	log.Printf("Executing command: %s %v", command, args)
	switch command {
	case "PING":
		return s.handlePing(args...)
	case "ECHO":
		return s.handleEcho(args...)
	case "SET":
		return s.handleSet(args...)
	case "GET":
		return s.handleGet(args...)
	case "INCR":
		return s.handleIncr(args...)
	case "DECR":
		return s.handleDecr(args...)
	case "SAVE":
		return s.handleSave()
	case "BGSAVE":
		return s.handleBgSave()
	default:
		return fmt.Sprintf("ERR unknown command '%s'", command)
	}
}

func (s *RedisServer) handlePing(args ...string) string {
	if len(args) > 1 {
		return "ERR wrong number of arguments for 'ping' command"
	}
	if len(args) == 0 {
		return "PONG"
	}
	return args[0]
}

func (s *RedisServer) handleEcho(args ...string) string {
	if len(args) != 1 {
		return "ERR wrong number of arguments for 'echo' command"
	}
	return args[0]
}

func (s *RedisServer) handleSet(args ...string) string {
	if len(args) < 2 {
		return "ERR wrong number of arguments for 'set' command"
	}
	key := args[0]
	value := args[1]
	s.mu.Lock()
	s.data[key] = value
	s.mu.Unlock()
	s.aofAppend("SET", key, value)
	return "OK"
}

func (s *RedisServer) handleGet(args ...string) string {
	if len(args) != 1 {
		return "ERR wrong number of arguments for 'get' command"
	}
	key := args[0]
	s.mu.RLock()
	value, ok := s.data[key]
	s.mu.RUnlock()
	if !ok {
		return "nil"
	}
	return value
}

func (s *RedisServer) handleIncr(args ...string) string {
	if len(args) != 1 {
		return "ERR wrong number of arguments for 'incr' command"
	}
	key := args[0]
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.data[key]
	if !ok {
		s.data[key] = "1"
		s.aofAppend("SET", key, "1")
		return "1"
	}
	intValue, err := strconv.Atoi(value)
	if err != nil {
		return "ERR value is not an integer"
	}
	intValue++
	s.data[key] = strconv.Itoa(intValue)
	s.aofAppend("SET", key, s.data[key])
	return s.data[key]
}

func (s *RedisServer) handleDecr(args ...string) string {
	if len(args) != 1 {
		return "ERR wrong number of arguments for 'decr' command"
	}
	key := args[0]
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.data[key]
	if !ok {
		s.data[key] = "-1"
		s.aofAppend("SET", key, "-1")
		return "-1"
	}
	intValue, err := strconv.Atoi(value)
	if err != nil {
		return "ERR value is not an integer"
	}
	intValue--
	s.data[key] = strconv.Itoa(intValue)
	s.aofAppend("SET", key, s.data[key])
	return s.data[key]
}

func (s *RedisServer) handleSave() string {
	if err := s.saveRDB(); err != nil {
		return "ERR " + err.Error()
	}
	return "OK"
}

func (s *RedisServer) handleBgSave() string {
	go s.saveRDB()
	return "Background saving started"
}

func (s *RedisServer) saveRDB() error {
	if !s.rdbSavingEnabled {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	file, err := os.Create(s.rdbTempFile)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := bufio.NewWriter(file)
	for key, value := range s.data {
		writer.WriteString(fmt.Sprintf("%s %s\n", key, value))
	}
	writer.Flush()
	os.Rename(s.rdbTempFile, s.rdbFile)
	return nil
}

func (s *RedisServer) loadRDB() error {
	file, err := os.Open(s.rdbFile)
	if err != nil {
		return err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	s.mu.Lock()
	defer s.mu.Unlock()
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, " ")
		if len(parts) != 2 {
			return fmt.Errorf("invalid RDB format")
		}
		key := parts[0]
		value := parts[1]
		s.data[key] = value
	}
	return nil
}

func (s *RedisServer) aofAppend(command string, args ...string) {
	s.aofMu.Lock()
	defer s.aofMu.Unlock()
	s.aofBuffer = append(s.aofBuffer, fmt.Sprintf("%s %s\n", command, strings.Join(args, " ")))
}

func (s *RedisServer) aofFlush() error {
	s.aofMu.Lock()
	defer s.aofMu.Unlock()
	file, err := os.OpenFile(s.aofFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := bufio.NewWriter(file)
	for _, command := range s.aofBuffer {
		writer.WriteString(command)
	}
	writer.Flush()
	s.aofBuffer = s.aofBuffer[:0]
	return nil
}

func (s *RedisServer) loadAOF() error {
	file, err := os.Open(s.aofFile)
	if err != nil {
		return err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, " ")
		if len(parts) == 0 {
			continue
		}
		command := parts[0]
		args := parts[1:]
		s.handleCommand(command, args...)
	}
	return nil
}

func (s *RedisServer) Run() {
	log.Printf("Redis server is running on %s:%d", s.host, s.port)
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", s.host, s.port))
	if err != nil {
		log.Fatalf("Error starting Redis server: %v", err)
	}
	defer listener.Close()
	if _, err := os.Stat(s.aofFile); err == nil {
		log.Printf("Loading AOF file: %s", s.aofFile)
		if err := s.loadAOF(); err != nil {
			log.Printf("Error loading AOF file: %v", err)
		}
	} else if _, err := os.Stat(s.rdbFile); err == nil {
		log.Printf("Loading RDB file: %s", s.rdbFile)
		if err := s.loadRDB(); err != nil {
			log.Printf("Error loading RDB file: %v", err)
		}
	}
	go func() {
		for {
			time.Sleep(1 * time.Second)
			s.aofFlush()
		}
	}()
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}
		go s.handleConnection(conn)
	}
}

func main() {
	server := NewRedisServer("127.0.0.1", 6379, "", 0, "appendonly.aof", "dump.rdb")
	server.Run()
}
