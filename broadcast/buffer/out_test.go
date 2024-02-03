package buffer_test

import (
	"broadcast/buffer"
	"reflect"
	"testing"
)

func TestAddMessage(t *testing.T) {
	o := buffer.NewOut()
	dst := "i"
	o.AddNode(dst)
	messages := []buffer.Message{
		{Payload: 0, Index: 0},
		{Payload: 1, Index: 1},
		{Payload: 2, Index: 2},
	}
	for _, msg := range messages {
		o.AddMessage(msg.Payload)
		actual := o.Read(dst)
		expected := messages[:msg.Index + 1]
		if !reflect.DeepEqual(actual, expected) {
			t.FailNow()
		}
	}
	for _, msg := range messages {
		o.Acknowledge(dst, msg.Index)
		actual := o.Read(dst)
		expected := messages[msg.Index +1:]
		if reflect.DeepEqual(actual, expected) {
			t.FailNow()
		}
	}
}
