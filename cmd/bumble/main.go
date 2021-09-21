// Main executable for bumblebase.
package main

import (
	//"flag"
	"fmt"

	config "github.com/brown-csci1270/db/pkg/config"
	list "github.com/brown-csci1270/db/pkg/list"
	repl "github.com/brown-csci1270/db/pkg/repl"
)

// Start the database.
func main() {
	l := list.NewList()
	fmt.Println(l)
	l.PushHead(2)
	fmt.Println(l.PeekHead().GetList(), l.PeekHead())
	l.PushHead(5)
	fmt.Println(l.PeekHead().GetList())
	l.PushTail(3)
	fmt.Println(l.PeekHead())
	l.PeekHead().PopSelf()
	for element := l.PeekHead(); element != nil; element = element.GetNext() {
		fmt.Println(element.GetKey())
	}
}
