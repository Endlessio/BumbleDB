package test

import (
	// "fmt"
	"testing"

	list "github.com/brown-csci1270/db/pkg/list"
)

func TestSample(t *testing.T) {
	l := list.NewList()
	l.PushHead(3)
	l.PushTail(1)
	if l.PeekHead() == nil || l.PeekTail() == nil {
		t.Fatal("bad list initialization")
	}
	
}