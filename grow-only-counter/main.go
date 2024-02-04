package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const TimeOut = time.Second

type Server struct {
	sync.RWMutex
	node *maelstrom.Node
	kv   *maelstrom.KV
}

func (s *Server) handleInit(msg maelstrom.Message) error {
	context, cancleFunc := context.WithTimeout(context.Background(), TimeOut)
	defer cancleFunc()
	s.kv.Write(context, msg.Dest, 0)
	return nil
}
func (s *Server) handleAdd(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	delta := int(body["delta"].(float64))
	readCtx, cancelRead := context.WithTimeout(context.Background(), TimeOut)
	defer cancelRead()
	s.Lock()
	value, err := s.kv.ReadInt(readCtx, msg.Dest)
	if err != nil {
		log.Println("ERROR could not read value err: ", err)
	}
	value += delta
	writeCtx, cancelWrite := context.WithTimeout(context.Background(), TimeOut)
	defer cancelWrite()
	s.kv.Write(writeCtx, msg.Dest, value)
	s.Unlock()
	return s.node.Reply(msg, map[string]any{
		"type": "add_ok",
	})
}
func (s *Server) handleRead(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	s.RLock()
	sum := 0
	for _, node := range s.node.NodeIDs() {
		ctx, cancel := context.WithTimeout(context.Background(), TimeOut)
		defer cancel()
		value, err := s.kv.ReadInt(ctx, node)
		if err != nil {
			log.Println("ERROR could not read value from: ", node, "  err: ", err)
		}
		sum += value
	}
	s.RUnlock()
	return s.node.Reply(msg, map[string]any{
		"type":  "read_ok",
		"value": sum,
	})
}

func main() {
	node := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(node)
	server := Server{node: node, kv: kv}
	node.Handle("init", server.handleInit)
	node.Handle("add", server.handleAdd)
	node.Handle("read", server.handleRead)
	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}
