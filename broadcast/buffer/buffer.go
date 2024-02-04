package buffer

import (
	"slices"
	"sync"
)

type Buffer struct {
	sync.RWMutex
	messages    []int
	vectorClock map[string]int
}

func NewOut() *Buffer {
	messages := make([]int, 0)
	vectorClock := make(map[string]int)
	return &Buffer{messages: messages, vectorClock: vectorClock}
}

func (b *Buffer) AddNode(node string) {
	b.Lock()
	defer b.Unlock()
	if _, ok := b.vectorClock[node]; !ok {
		b.vectorClock[node] = 0
	}
}
func (b *Buffer) AddMessage(payload int) {
	b.Lock()
	defer b.Unlock()
	if !slices.Contains(b.messages, payload) {
		b.messages = append(b.messages, payload)
	}
}

func (b *Buffer) Acknowledge(node string, index int) {
	b.Lock()
	defer b.Unlock()
	b.vectorClock[node] = index
}

func (b *Buffer) Read(node string) ([]int, int) {
	b.RLock()
	defer b.RUnlock()
	begin := b.vectorClock[node]
	messages := make([]int, 0)
	for i := begin; i < len(b.messages); i++ {
		message := b.messages[i]
		messages = append(messages, message)
	}
	return messages, len(b.messages)
}

func (b *Buffer) ReadAll() []int {
	b.RLock()
	defer b.RUnlock()
	return b.messages
}
