package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// type History struct {
// 	sync.RWMutex
// 	inner map[string]map[int]bool
// }

// func (m *History) AddNode(id string) {
// 	m.RLock()
// 	_, ok := m.inner[id]
// 	m.RUnlock()
// 	if !ok {
// 		m.Lock()
// 		m.inner[id] = make(map[int]bool)
// 		m.Unlock()
// 	}
// }
// func (m *History) AddMsgID(id string, msgID int) {
// 	m.Lock()
// 	m.inner[id][msgID] = true
// 	m.Unlock()
// }
// func (m *History) Contains(id string, msgID int) bool {
// 	m.RLock()
// 	defer m.RUnlock()
// 	_, ok := m.inner[id][msgID]
// 	return ok
// }

// type Message struct {
// 	Content int
// 	Src     string
// }

type MessageStore struct {
	sync.RWMutex
	messages    []int
	vectorClock map[string]int
}

func (b *MessageStore) AddNode(id string) {
	b.RLock()
	_, ok := b.vectorClock[id]
	b.RUnlock()
	if !ok {
		b.Lock()
		b.vectorClock[id] = 0
		b.Unlock()
	}
}
func (b *MessageStore) UpdateNode(src string, time int) {
	b.Lock()
	b.vectorClock[src] = time
	b.Unlock()
}

func (b *MessageStore) AddMessage(src string, content int) {
	b.Lock()
	b.messages = append(b.messages, content)
	b.Unlock()
}
func (b *MessageStore) read(id string) []int {
	b.RLock()
	lastRead := b.vectorClock[id]
	amount := len(b.messages)
	if lastRead < amount {
		messages := make([]int, amount-lastRead)
		for i := lastRead; i < amount; i++ {
			messages = append(messages, b.messages[i])
		}
		b.RUnlock()
		b.UpdateNode(id, amount)
		return messages
	} else {
		b.RUnlock()
		return make([]int, 0)
	}
}

func (b *MessageStore) ReadAll() []int {
	b.RLock()
	defer b.RUnlock()
	messages := make([]int, len(b.messages))
	messages = append(messages, b.messages...)
	// for _, msg := range b.messages {
	// 	messages = append(messages, msg)
	// }
	return messages
}

type Server struct {
	node  *maelstrom.Node
	store *MessageStore
}

func (s *Server) handleBroadcast(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	message := int(body["message"].(float64))
	s.store.AddMessage(msg.Src, message)
	payload := make(map[string]any)
	payload["type"] = "replicate"
	payload["message"] = message
	go s.sendBroadcast(payload)
	return s.node.Reply(msg, map[string]any{
		"type": "broadcast_ok",
	})
}
func (s *Server) sendBroadcast(payload map[string]any) {
	for _, dest := range s.node.NodeIDs() {
		if dest != s.node.ID() {
			s.node.RPC(dest, payload, func(msg maelstrom.Message) error { return nil })
		}
	}

}
func (s *Server) handleRead(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	messages := s.store.ReadAll()
	return s.node.Reply(msg, map[string]any{
		"type":     "read_ok",
		"messages": messages,
	})
}
func (s *Server) handleTopology(msg maelstrom.Message) error {
	s.store.AddNode(msg.Src)
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	for _, other := range s.node.NodeIDs() {
		if other != s.node.ID() {
			s.store.AddNode(other)
		}
	}
	return s.node.Reply(msg, map[string]any{
		"type": "topology_ok",
	})
}

func (s *Server) handleReplicate(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	message := int(body["message"].(float64))
	s.store.AddMessage(msg.Src, message)
	return s.node.Reply(msg, map[string]any{
		"type": "replicate_ok",
	})
}
func main() {
	n := maelstrom.NewNode()
	broadcast := MessageStore{messages: make([]int, 0), vectorClock: make(map[string]int)}
	// history := History{inner: make(map[string]map[int]bool)}
	server := Server{node: n, store: &broadcast}

	n.Handle("broadcast", server.handleBroadcast)
	n.Handle("read", server.handleRead)
	n.Handle("topology", server.handleTopology)
	n.Handle("replicate", server.handleReplicate)
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
