// Main executable for bumblebase.
package main

import (
	"flag"
	// "fmt"

	uuid "github.com/google/uuid"
	config "github.com/brown-csci1270/db/pkg/config"
	list "github.com/brown-csci1270/db/pkg/list"
	// repl "github.com/brown-csci1270/db/pkg/repl"
)

var (
	_list bool
	_c bool
)

func init() {
	flag.BoolVar(&_list, "list", false, "bool flag value")
	flag.BoolVar(&_c, "c", false, "bool flag value")
  }

// Start the database.
func main() {
	flag.Parse()

	l := list.NewList()
	l.PushHead(2)
	l.PushHead(5)
	l.PushTail(3)
	l.PeekHead().PopSelf()

	// r := repl.NewRepl()

	prompt := config.GetPrompt(_c)

	uuid := uuid.New()
	if _list{
		list.ListRepl(l).Run(nil, uuid, prompt)
	}
}