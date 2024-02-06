package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"sync"
)

type Server struct {
	sync.RWMutex
	node     *maelstrom.Node
	logs     map[string][]int
	commited map[string]int
}

func (s *Server) handleSend(msg maelstrom.Message) error {
	body := struct {
		Key string `json:"key"`
		Msg int    `json:"msg"`
	}{}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	s.Lock()
	if _, ok := s.logs[body.Key]; !ok {
		log := make([]int, 0, 1)
		s.logs[body.Key] = log
		s.commited[body.Key] = -1
	}
	offset := len(s.logs[body.Key])
	s.logs[body.Key] = append(s.logs[body.Key], body.Msg)
	s.Unlock()
	return s.node.Reply(msg, map[string]any{
		"type":   "send_ok",
		"offset": offset,
	})
}
func (s *Server) handlePoll(msg maelstrom.Message) error {
	body := struct {
		Offsets map[string]int `json:"offsets"`
	}{}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	s.RLock()
	messages := make(map[string][][2]int)
	for key, offset := range body.Offsets {
		if log, ok := s.logs[key]; ok && offset < len(log) {
			entries := make([][2]int, 0)
			for i := offset; i < len(log); i++ {
				entry := [2]int{i, log[i]}
				entries = append(entries, entry)
			}
			messages[key] = entries
		}
	}
	s.RUnlock()
	return s.node.Reply(msg, map[string]any{
		"type": "poll_ok",
		"msgs": messages,
	})
}
func (s *Server) handleCommitOffsets(msg maelstrom.Message) error {
	body := struct {
		Offsets map[string]int `json:"offsets"`
	}{}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	s.Lock()
	for key, commited := range body.Offsets {
		if entry, ok := s.commited[key]; ok && commited > entry {
			s.commited[key] = commited
		}
	}
	s.Unlock()
	return s.node.Reply(msg, map[string]any{
		"type": "commit_offsets_ok",
	})
}
func (s *Server) handleListCommitedOffsets(msg maelstrom.Message) error {
	body := struct {
		Keys []string `json:"keys"`
	}{}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	offsets := make(map[string]int)
	s.RLock()
	for _, key := range body.Keys {
		if offset, ok := s.commited[key]; ok {
			offsets[key] = offset
		}
	}
	s.RUnlock()
	return s.node.Reply(msg, map[string]any{
		"type":    "list_committed_offsets_ok",
		"offsets": offsets,
	})
}

func main() {
	n := maelstrom.NewNode()
	logs := make(map[string][]int)
	commited := make(map[string]int)
	server := Server{node: n, logs: logs, commited: commited}

	n.Handle("send", server.handleSend)
	n.Handle("poll", server.handlePoll)
	n.Handle("commit_offsets", server.handleCommitOffsets)
	n.Handle("list_committed_offsets", server.handleListCommitedOffsets)
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
