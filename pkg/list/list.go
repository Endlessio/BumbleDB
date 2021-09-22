package list

import (
	"errors"
	"fmt"

	// "io"
	"strings"
	"strconv"

	repl "github.com/brown-csci1270/db/pkg/repl"
)

// List struct.
type List struct {
	head *Link
	tail *Link
}

// Create a new list.
func NewList() *List {
	list := new(List)
	list.head = nil
	list.tail = nil
	return list
}

// Get a pointer to the head of the list.
func (list *List) PeekHead() *Link {
	return list.head
}

// Get a pointer to the tail of the list.
func (list *List) PeekTail() *Link {
	return list.tail
}

// Add an element to the start of the list. Returns the added link.
func (list *List) PushHead(value interface{}) *Link {
	newLink := &Link{
		value: value,
	}
	newLink.list = list
	if list.head == nil && list.tail == nil{
		list.head = newLink
		list.tail = newLink
	}else if list.head == nil{
		list.head = newLink
		newLink.next = list.tail
	}else{
		newLink.next = list.head
		list.head.prev = newLink
		list.head = newLink
	}
	return newLink
}

// Add an element to the end of the list. Returns the added link.
func (list *List) PushTail(value interface{}) *Link {
	newLink := &Link{
		value: value,
	}
	newLink.list = list
	if list.tail == nil && list.head == nil{
		list.head = newLink
		list.tail = newLink
	}else if list.tail == nil{
		list.tail = newLink
		newLink.prev = list.head
	}else{
		newLink.prev = list.tail
		list.tail.next = newLink
		list.tail = newLink
	}
	return newLink
}

// Find an element in a list given a boolean function, f, that evaluates to true on the desired element.
func (list *List) Find(f func(*Link) bool) *Link {
	cur := list.head
	for cur!=nil {
		if f(cur) {
			return cur
		}else{
			cur = cur.next
		}
	}
	return nil
}

// Apply a function to every element in the list. f should alter Link in place.
func (list *List) Map(f func(*Link)) {
	cur := list.head
	for cur!=nil {
		f(cur)
		cur = cur.next
	}
}

// Link struct. ie Node
type Link struct {
	list  *List
	prev  *Link
	next  *Link
	value interface{}
}

// Get the list that this link is a part of.
func (link *Link) GetList() *List {
	return link.list
}

// Get the link's value.
func (link *Link) GetKey() interface{} {
	return link.value
}

// Set the link's value.
func (link *Link) SetKey(value interface{}) {
	link.value = value
}

// Get the link's prev.
func (link *Link) GetPrev() *Link {
	res := link.prev
	return res
}

// Get the link's next.
func (link *Link) GetNext() *Link {
	res := link.next
	return res
}

// Remove this link from its list.
func (link *Link) PopSelf() {
	// it is head
	if link == nil{
	}else if link == link.list.head {
		cur := link.next
		if cur != nil{
			link.list.head = cur
		}else{
			link.list.head = nil
			link.list.tail = nil
		}
	}else if link == link.list.tail {
		cur := link.prev
		if cur != nil{
			link.list.tail = cur
		}else{
			link.list.head = nil
			link.list.tail = nil
		}
	}else{
		left := link.prev
		right := link.next
		left.next = right
		right.prev = left
	}
}

// List REPL.
func ListRepl(list *List) *repl.REPL {
	user := repl.NewRepl()

	list_print := func(str string, none *repl.REPLConfig) error{
		res := ""
		cur := list.head
		for cur!=nil{
			temp := cur.value
			switch temp.(type){
				case int:
					temp = strconv.Itoa(temp.(int))
				default:
					temp = temp.(string)
			}
			res += temp.(string)+","
			cur = cur.next
		}
		if res == ""{
			fmt.Println(errors.New("empty list"))
		}
		fmt.Println(res)
		return errors.New(res)
	}

	list_push_head := func(str string, none *repl.REPLConfig) error{
		splitted := strings.Split(str, " ")
		element := splitted[1]
		list.PushHead(element)
		return errors.New("the element has been pushed to head")
	}


	list_push_tail := func(str string, none *repl.REPLConfig) error{
		splitted := strings.Split(str, " ")
		element := splitted[1]
		list.PushTail(element)
		return errors.New("the element has been pushed to tail")
	}

	list_remove := func(str string, none *repl.REPLConfig) error{
		splitted := strings.Split(str, " ")
		element := splitted[1]
		cur := list.head
		for cur!=nil{
			temp := cur.value
			switch temp.(type){
				case int:
					temp = strconv.Itoa(temp.(int))
				default:
					temp = temp.(string)
			}
			if temp.(string) == element{
				cur.PopSelf()
				return errors.New("the element has been removed")
			}
			cur = cur.next
		}
		fmt.Println("the element not existed")
		return errors.New("the element not existed")
	}

	list_contains := func(str string, none *repl.REPLConfig) error{
		splitted := strings.Split(str, " ")
		element := splitted[1]
		cur := list.head
		for cur!=nil{
			temp := cur.value
			switch temp.(type){
				case int:
					temp = strconv.Itoa(temp.(int))
				default:
					temp = temp.(string)
			}
			if temp.(string) == element{
				fmt.Println("found!")
				return errors.New("the list_contains has been exe")
			}
			cur = cur.next
		}
		fmt.Println("not found!")
		return errors.New("the list_contains has been exe")
	}

	user.AddCommand("list_print", list_print, "Prints out all of the elements in the list in order, separated by commas")
	user.AddCommand("list_push_head", list_push_head, "Inserts the given element to the List as a string.")
	user.AddCommand("list_push_tail", list_push_tail, "Inserts the given element to the end of the List as a string")
	user.AddCommand("list_remove", list_remove, "Removes the given element from the list.")
	user.AddCommand("list_contains", list_contains, "Prints \"found!\" if the element is in the list, prints \"not found\" otherwise.")
	return user
}