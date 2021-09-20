// Main executable for bumblebase.
package main

import (
	//"flag"
	"fmt"

	//config "github.com/brown-csci1270/db/pkg/config"
	list "github.com/brown-csci1270/db/pkg/list"
	//repl "github.com/brown-csci1270/db/pkg/repl"
)

// Start the database.
func main() {
	l := list.NewList()
	l.PushHead(2)
	l.PushTail(1)
	l.PushHead(3)
	for element := l.PeekTail().GetList().PeekHead(); element != nil; element = element.value() {
		fmt.Println(element.value)
	}
}
