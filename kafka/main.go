package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const CommitedSuffix = "_comited"
const OffsetSuffix = "_offset"

type Server struct {
	sync.RWMutex
	node       *maelstrom.Node
	logs       *maelstrom.KV
	logsCached map[string][]int
	commited   *maelstrom.KV
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
	offset, err := s.commited.ReadInt(context.Background(), body.Key+OffsetSuffix)
	if err != nil {
		offset = -1
	}
	for {
		err := s.commited.CompareAndSwap(context.Background(), body.Key+OffsetSuffix, offset, offset+1, true)
		offset++
		if err == nil {
			break
		}
	}
	key := fmt.Sprint(body.Key, "_", offset)
	if err := s.logs.Write(context.Background(), key, body.Msg); err != nil {
		return err
	}

	log, ok := s.logsCached[body.Key]
	if !ok {
		s.logsCached[body.Key] = make([]int, offset+1)
	}
	for len(log) <= offset {
		log = append(log, -1)
	}
	log[offset] = body.Msg
	s.logsCached[body.Key] = log
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
	s.Lock()
	messages := make(map[string][][2]int)
	for key, offset := range body.Offsets {
		for i := len(s.logsCached[key]); i < offset+10; i++ {
			keyKV := fmt.Sprint(key, "_", i)
			entry, err := s.logs.ReadInt(context.Background(), keyKV)
			if err != nil {
				break
			}
			s.logsCached[key] = append(s.logsCached[key], entry)
		}
		entries := make([][2]int, 0)
		for i := offset; i < len(s.logsCached[key]); i++ {
			if s.logsCached[key][i] == -1 {
				keyKV := fmt.Sprint(key, "_", i)
				value, err := s.logs.ReadInt(context.Background(), keyKV)
				if err != nil {
					return err
				}
				s.logsCached[key][i] = value
			}
			entry := [2]int{i, s.logsCached[key][i]}
			entries = append(entries, entry)
		}
		messages[key] = entries
	}
	s.Unlock()
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
		keyKV := key + CommitedSuffix
		entry, err := s.commited.ReadInt(context.Background(), keyKV)
		if err != nil {
			s.commited.CompareAndSwap(context.Background(), keyKV, commited, commited, true)
			entry, _ = s.commited.ReadInt(context.Background(), keyKV)
		}
		for entry < commited {
			if err = s.commited.CompareAndSwap(context.Background(), keyKV, entry, commited, true); err != nil {
				break
			}
			entry, err = s.commited.ReadInt(context.Background(), keyKV)
			if err != nil {
				log.Println("ERROR should not be reached! Commit entry for key: ", key, " does not exist err: ", err.Error())
				return err
			}
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
		keyKV := key + CommitedSuffix
		offset, err := s.commited.ReadInt(context.Background(), keyKV)
		if err == nil {
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
	logs := maelstrom.NewSeqKV(n)
	commited := maelstrom.NewLinKV(n)
	logsCached := make(map[string][]int)
	server := Server{node: n, logs: logs, commited: commited, logsCached: logsCached}

	n.Handle("send", server.handleSend)
	n.Handle("poll", server.handlePoll)
	n.Handle("commit_offsets", server.handleCommitOffsets)
	n.Handle("list_committed_offsets", server.handleListCommitedOffsets)
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
