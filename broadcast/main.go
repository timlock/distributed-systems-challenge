package main

import (
	"broadcast/buffer"
	"encoding/json"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Server struct {
	node       *maelstrom.Node
	buffer     *buffer.Buffer
	neighbours []string
}

func (s *Server) Run() {
	go func() {
		ticker := time.NewTicker(800 * time.Millisecond)
		for {
			<-ticker.C
			body := struct {
				Type     string `json:"type"`
				Messages []int
				Index    int
			}{Type: "replicate"}
			for _, dst := range s.neighbours {
				messages, lastIndex := s.buffer.Read(dst)
				if len(messages) == 0 {
					continue
				}
				body.Messages = messages
				body.Index = lastIndex
				s.node.RPC(dst, body, func(msg maelstrom.Message) error {
					body := struct{ Ack int }{}
					if err := json.Unmarshal(msg.Body, &body); err != nil {
						return err
					}
					lastIndex = body.Ack
					s.buffer.Acknowledge(msg.Src, lastIndex)
					log.Println("Updated vector clock: ", msg.Src, ": ", lastIndex+1)
					return nil
				})

			}
		}
	}()
}

func (s *Server) handleBroadcast(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	payload := int(body["message"].(float64))
	s.buffer.AddMessage(payload)
	return s.node.Reply(msg, map[string]any{
		"type": "broadcast_ok",
	})
}

func (s *Server) handleReplicate(msg maelstrom.Message) error {
	body := struct {
		Messages []int
		Index    int
	}{}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	messages := body.Messages
	for _, message := range messages {
		s.buffer.AddMessage(message)
	}
	return s.node.Reply(msg, map[string]any{
		"type": "replicate_ok",
		"Ack":  body.Index,
	})
}
func (s *Server) handleRead(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	messages := s.buffer.ReadAll()
	return s.node.Reply(msg, map[string]any{
		"type":     "read_ok",
		"messages": messages,
	})
}
func (s *Server) handleTopology(msg maelstrom.Message) error {
	body := struct {
		Topology map[string][]string `json:"topology"`
	}{}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	s.neighbours = s.node.NodeIDs()
	log.Println("Neighbours are: ", body.Topology[msg.Dest])
	return s.node.Reply(msg, map[string]any{
		"type": "topology_ok",
	})
}

func main() {
	n := maelstrom.NewNode()
	outgoing := buffer.NewBuffer()
	server := Server{node: n, buffer: outgoing}
	server.Run()

	n.Handle("broadcast", server.handleBroadcast)
	n.Handle("read", server.handleRead)
	n.Handle("topology", server.handleTopology)
	n.Handle("replicate", server.handleReplicate)
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
