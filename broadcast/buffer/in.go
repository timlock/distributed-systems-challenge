package buffer

import (
	"sync"
)

type In struct {
	sync.RWMutex
	messages map[string][]int
}

func NewIn() *In {
	messages := make(map[string][]int)
	return &In{messages: messages}
}

func (i *In) Add(message Message, src string) {
	i.Lock()
	defer i.Unlock()
	if _, ok := i.messages[src]; !ok {
		i.messages[src] = make([]int, 0, message.Index+1)
	}
	begin := len(i.messages[src])
	for j := begin; j <= message.Index; j++ {
		i.messages[src] = append(i.messages[src], 0)
	}
	i.messages[src][message.Index] = message.Payload
}

func (i *In) ReadAll() []int {
	all := make([]int, 0)
	i.RLock()
	defer i.RUnlock()
	for _, messages := range i.messages {
		all = append(all, messages...)
	}
	return all
}
