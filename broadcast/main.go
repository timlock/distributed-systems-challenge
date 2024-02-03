package main

import (
	"broadcast/buffer"
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Server struct {
	node     *maelstrom.Node
	outgoing *buffer.Out
	ingoing  *buffer.In
}

func (s *Server) handleBroadcast(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	payload := int(body["message"].(float64))
	s.outgoing.AddMessage(payload)

	go s.sendBroadcast()
	return s.node.Reply(msg, map[string]any{
		"type": "broadcast_ok",
	})
}
func (s *Server) sendBroadcast() {
	body := make(map[string]any)
	body["type"] = "replicate"
	for _, dst := range s.node.NodeIDs() {
		if dst != s.node.ID() {
			messages := s.outgoing.Read(dst)
			if len(messages) == 0 {
				continue
			}
			body["Messages"] = messages
			lastIndex := messages[len(messages)-1].Index
			s.node.RPC(dst, body, func(msg maelstrom.Message) error {
				var body map[string]any
				if err := json.Unmarshal(msg.Body, &body); err != nil {
					return err
				}
				s.outgoing.Acknowledge(dst, lastIndex)
				return nil
			})
		}
	}

}

func (s *Server) handleReplicate(msg maelstrom.Message) error {
	body := struct {
		Messages []buffer.Message
	}{}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	messages := body.Messages
	for _, message := range messages {
		s.ingoing.Add(message, msg.Src)
	}
	return s.node.Reply(msg, map[string]any{
		"type": "replicate_ok",
	})
}
func (s *Server) handleRead(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	sent := s.outgoing.ReadAll()
	received := s.ingoing.ReadAll()
	messages := make([]int, 0, len(sent)+len(received))
	messages = append(messages, sent...)
	messages = append(messages, received...)
	return s.node.Reply(msg, map[string]any{
		"type":     "read_ok",
		"messages": messages,
	})
}
func (s *Server) handleTopology(msg maelstrom.Message) error {
	s.outgoing.AddNode(msg.Src)
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	for _, other := range s.node.NodeIDs() {
		if other != s.node.ID() {
			s.outgoing.AddNode(other)
		}
	}
	return s.node.Reply(msg, map[string]any{
		"type": "topology_ok",
	})
}

func main() {
	n := maelstrom.NewNode()
	outgoing := buffer.NewOut()
	ingoing := buffer.NewIn()
	server := Server{node: n, outgoing: outgoing, ingoing: ingoing}

	n.Handle("broadcast", server.handleBroadcast)
	n.Handle("read", server.handleRead)
	n.Handle("topology", server.handleTopology)
	n.Handle("replicate", server.handleReplicate)
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
