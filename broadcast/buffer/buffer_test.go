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
	messages := []int{1, 2, 3}
	for index, msg := range messages {
		o.AddMessage(msg)
		actual, lastRead := o.Read(dst)
		if lastRead != index+1 {
			t.Fatal("expected: ", index+1, " actual: ", lastRead)
		}
		expected := messages[:index+1]
		if !reflect.DeepEqual(actual, expected) {
			t.Fatal("expected: ", expected, " actual: ", actual)
		}
	}
	for index, _ := range messages {
		o.Acknowledge(dst, index)
		actual, _ := o.Read(dst)
		expected := messages[index+1:]
		if !reflect.DeepEqual(actual, expected) {
			t.Fatal("expected: ", expected, " actual: ", actual)
		}
	}
}
