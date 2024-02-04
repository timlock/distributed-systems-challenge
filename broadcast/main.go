package main

import (
	"broadcast/buffer"
	"encoding/json"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Message int
type Messages []int
type Acknowledge struct {
	Index int
	Src   string
}
type NewNode string
type Read chan []int
type Topology []string

type Server struct {
	node         *maelstrom.Node
	buffer       *buffer.Buffer
	neighbours   []string
	instructions chan any
}

func (s *Server) Run() {
	go func() {
		ticker := time.NewTicker(800 * time.Millisecond)
		for {
			// select {
			// case <-ticker.C:
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
					// s.instructions <- Acknowledge{Src: msg.Src, Index: lastIndex}
					log.Println("Updated vector clock: ", msg.Src, ": ", lastIndex+1)
					return nil
				})

				// }
				// case msg := <-s.instructions:
				// 	switch t := msg.(type) {
				// 	case Message:
				// 		s.buffer.AddMessage(int(t))
				// 	case Messages:
				// 		for _, message := range t {
				// 			s.buffer.AddMessage(message)
				// 		}
				// 	case Acknowledge:
				// 		s.buffer.Acknowledge(t.Src, t.Index)
				// 	case NewNode:
				// 		s.buffer.AddNode(string(t))
				// 	case Read:
				// 		t <- s.buffer.ReadAll()
				// 	case Topology:
				// 		s.neighbours = t
				// 	default:
				// 		log.Fatalln("Received unknown message: ", t)
				// 	}
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
	// s.instructions <- Message(payload)
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
	// s.instructions <- Messages(messages)
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
	// c := make(chan []int)
	// s.instructions <- Read(c)
	// messages := <-c
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
	// s.neighbours = body.Topology[msg.Dest]
	// s.instructions <- Topology(body.Topology[msg.Dest])
	log.Println("Neighbours are: ", body.Topology[msg.Dest])
	return s.node.Reply(msg, map[string]any{
		"type": "topology_ok",
	})
}

func main() {
	n := maelstrom.NewNode()
	outgoing := buffer.NewOut()
	instructions := make(chan any)
	server := Server{node: n, buffer: outgoing, instructions: instructions}
	server.Run()

	n.Handle("broadcast", server.handleBroadcast)
	n.Handle("read", server.handleRead)
	n.Handle("topology", server.handleTopology)
	n.Handle("replicate", server.handleReplicate)
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
