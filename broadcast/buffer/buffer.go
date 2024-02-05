package buffer

import (
	"sync"
)

type Buffer struct {
	sync.RWMutex
	messages    []int
	vectorClock map[string]int
	filter      map[int]struct{}
}

func NewBuffer() *Buffer {
	messages := make([]int, 0)
	vectorClock := make(map[string]int)
	filter := make(map[int]struct{})
	return &Buffer{messages: messages, vectorClock: vectorClock, filter: filter}
}

func (b *Buffer) AddNode(node string) {
	b.Lock()
	defer b.Unlock()
	if _, ok := b.vectorClock[node]; !ok {
		b.vectorClock[node] = 0
	}
}
func (b *Buffer) AddMessage(value int) {
	b.Lock()
	defer b.Unlock()
	if _, ok := b.filter[value]; !ok {
		b.messages = append(b.messages, value)
		b.filter[value] = struct{}{}
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
