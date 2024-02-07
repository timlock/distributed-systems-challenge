package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Server struct {
	sync.RWMutex
	node  *maelstrom.Node
	inner map[int]int
}

func (s *Server) handleTxn(msg maelstrom.Message) error {

	body := struct {
		Txn [][]any `json:"txn"`
	}{}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	s.Lock()
	for index, operation := range body.Txn {
		typ := operation[0].(string)
		key := int(operation[1].(float64))
		switch typ {
		case "r":
			value, ok := s.inner[key]
			if !ok {
				operation[2] = nil
			} else {
				operation[2] = value
			}
		case "w":
			s.inner[key] = int(operation[2].(float64))
		default:
			log.Println("ERROR received invalid operation: ", operation)
		}
		body.Txn[index] = operation
	}
	s.Unlock()
	return s.node.Reply(msg, map[string]any{
		"type": "txn_ok",
		"txn":  body.Txn,
	})
}

func main() {
	n := maelstrom.NewNode()
	inner := make(map[int]int)
	server := Server{node: n, inner: inner}

	n.Handle("txn", server.handleTxn)
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
