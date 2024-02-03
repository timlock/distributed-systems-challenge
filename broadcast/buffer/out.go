package buffer

import (
	"sync"
)

type Out struct {
	sync.RWMutex
	messages    []int
	vectorClock map[string]int
}

func NewOut() *Out {
	messages := make([]int, 0)
	vectorClock := make(map[string]int)
	return &Out{messages: messages, vectorClock: vectorClock}
}

func (o *Out) AddNode(dst string) {
	o.Lock()
	defer o.Unlock()
	if _, ok := o.vectorClock[dst]; !ok {
		o.vectorClock[dst] = 0
	}
}
func (o *Out) AddMessage(payload int) {
	o.Lock()
	defer o.Unlock()
	o.messages = append(o.messages, payload)
}

func (o *Out) Acknowledge(dst string, index int) {
	o.Lock()
	o.vectorClock[dst] = index + 1
	o.Unlock()
}

func (o *Out) Read(dst string) []Message {
	o.RLock()
	defer o.RUnlock()
	begin := o.vectorClock[dst]
	if begin == len(o.messages) {
		return make([]Message, 0)
	}
	messages := make([]Message, 0, len(o.messages[begin:]))
	for index, payload := range o.messages[begin:] {
		messages = append(messages, Message{Payload: payload, Index: index})
	}
	return messages
}

func (o *Out) ReadAll() []int {
	o.RLock()
	defer o.RUnlock()
	return o.messages
}
