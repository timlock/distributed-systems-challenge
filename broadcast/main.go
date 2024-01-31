package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"sync"
)

type Broadcast struct {
	messages      []int
	versionVector map[string]int
	sync.RWMutex
}

func (b *Broadcast) addNode(id string) {
	b.RLock()
	_, ok := b.versionVector[id]
	b.RUnlock()
	if !ok {
		b.Lock()
		b.versionVector[id] = 0
		b.Unlock()
	}
}
func (b *Broadcast) updateNode(id string, time int) {
	b.Lock()
	b.versionVector[id] = time
	b.Unlock()
}

func (b *Broadcast) addMessage(id string, message int) {
	b.Lock()
	b.messages = append(b.messages, message)
	b.Unlock()
}
func (b *Broadcast) read(id string) []int {
	b.RLock()
	lastRead := b.versionVector[id]
	amount := len(b.messages)
	if lastRead < amount {
		messages := make([]int, amount-lastRead)
		for i := lastRead; i < amount; i++ {
			messages = append(messages, b.messages[i])
		}
		b.RUnlock()
		b.updateNode(id, amount)
		return messages
	} else {
		b.RUnlock()
		return make([]int, 0)
	}
}

func main() {
	n := maelstrom.NewNode()
	broadcast := Broadcast{messages: make([]int, 20*10), versionVector: make(map[string]int)}
	n.Handle("init", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		for _, other := range n.NodeIDs(){
			if other != msg.Dest{
				broadcast.addNode(other)
			}
		}
		return n.Reply(msg, map[string]any{
			"type":        "init_ok",
			"in_reply_to": 1,
		})
	})
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		message := int(body["message"].(float64))
		broadcast.addMessage(msg.Src, message)
		for _, other := range n.NodeIDs() {
			if other != msg.Dest {
				n.Send(other, body)
			}
		}
		return n.Reply(msg, map[string]any{
			"type": "broadcast_ok",
		})
	})
	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		messages := broadcast.read(msg.Src)
		return n.Reply(msg, map[string]any{
			"type":     "read_ok",
			"messages": messages,
		})
	})
	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		return n.Reply(msg, map[string]any{
			"type": "topology_ok",
		})
	})
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
