package btree

import (
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"

	pager "github.com/brown-csci1270/db/pkg/pager"
)

// Split is a supporting data structure to propagate keys up our B+ tree.
type Split struct {
	isSplit bool  // A flag that's set if a split occurs.
	key     int64 // The key to promote.
	leftPN  int64 // The pagenumber for the left node.
	rightPN int64 // The pagenumber for the right node.
	err     error // Used to propagate errors upwards.
}

// Node defines a common interface for leaf and internal nodes.
type Node interface {
	// Interface for main node functions.
	search(int64) int64
	insert(int64, int64, bool) Split
	delete(int64)
	get(int64) (int64, bool)

	// Interface for helper functions.
	keyToNodeEntry(int64) (*LeafNode, int64, error)
	printNode(io.Writer, string, string)
	getPage() *pager.Page
	getNodeType() NodeType
}

/////////////////////////////////////////////////////////////////////////////
///////////////////////////// Leaf Node Methods /////////////////////////////
/////////////////////////////////////////////////////////////////////////////

// search returns the first index where key >= given key.
// If no key satisfies this condition, returns numKeys.
func (node *LeafNode) search(key int64) int64 {
	ind := sort.Search(int(node.numKeys), func(index int) bool { 
		return node.getKeyAt(int64(index)) >= key 
	})
	return int64(ind)
}

// insert finds the appropriate place in a leaf node to insert a new tuple.
// if update is true, allow overwriting existing keys. else, error.
func (node *LeafNode) insert(key int64, value int64, update bool) Split {
	node.unlockParent(false)
	defer node.unlock()
	idx := node.search(key)

	if idx < node.numKeys && node.getKeyAt(idx) == key {
		if update {
			node.updateValueAt(idx, value)
			// node.unlockParent(true)
			return Split{}
		} else {
			// node.unlockParent(true)
			return Split{err: errors.New("node/insertleaf: duplicated but not update")}
		}
	} else {
		if update {
			// fmt.Println("yes")
			// node.unlockParent(true)
			return Split{err: errors.New("node/insertleaf: update non-exist")}
		}
		for i:=node.numKeys-1; i>=idx; i-- {
			key_val := node.getKeyAt(i)
			val_val := node.getValueAt(i)
			node.updateKeyAt(i+1, key_val)
			node.updateValueAt(i+1, val_val)
		}
		node.updateKeyAt(idx, key)
		node.updateValueAt(idx, value)
		// update number of keys
		node.updateNumKeys(node.numKeys+1)
	}
	if node.numKeys>ENTRIES_PER_LEAF_NODE {
		res := node.split()
		// defer node.unlockParent(true)
		return res
	} else {
		// node.unlockParent(true)
		return Split{isSplit: false}
	}
}

// delete removes a given tuple from the leaf node, if the given key exists.
func (node *LeafNode) delete(key int64) {
	node.unlockParent(true)
	defer node.unlock()
	ind := node.search(key)
	// exist 
	if ind < node.numKeys && node.getKeyAt(ind) == key{
		for i:=int64(ind); i<=int64(node.numKeys)-2; i++{
			key_val:=node.getKeyAt(i+1)
			val_val:=node.getValueAt(i+1)
			node.updateKeyAt(i, key_val)
			node.updateValueAt(i, val_val)
			// node.modifyCell(i, BTreeEntry{key: key_val, value: val_val})
		}
		node.updateNumKeys(node.numKeys-1)
	} else {
		return
	}
}

// split is a helper function to split a leaf node, then propagate the split upwards.
func (node *LeafNode) split() Split {
	res := Split{}
	res.isSplit = true
	res.key = node.getKeyAt(node.numKeys/2)
	newNode, err := createLeafNode(node.page.GetPager())
	defer newNode.page.Put()
	if err != nil {
		res.err = errors.New("node/split: fail to split node")
		return Split{}
	}
	// add into new node
	for i:=node.numKeys/2; i<node.numKeys; i++{
		cur_val := node.getValueAt(i)
		cur_key := node.getKeyAt(i)
		newNode.updateKeyAt(newNode.numKeys, cur_key)
		newNode.updateValueAt(newNode.numKeys, cur_val)
		newNode.updateNumKeys(newNode.numKeys+1)
		// fmt.Println("check", i, cur_val, cur_key, newNode.numKeys, newNode.getKeyAt(0), newNode.getKeyAt(1))
	}
	newNode.setRightSibling(node.rightSiblingPN)
	// delete part of the original node data
	node.updateNumKeys(node.numKeys/2)
	node.setRightSibling(newNode.page.GetPageNum())

	right_page_num := newNode.page.GetPageNum()
	res.rightPN = right_page_num
	left_page_num := node.page.GetPageNum()
	res.leftPN = left_page_num
	return res
}

// get returns the value associated with a given key from the leaf node.
func (node *LeafNode) get(key int64) (value int64, found bool) {
	node.unlockParent(true)
	defer node.unlock()
	index := node.search(key)
	if index >= node.numKeys || node.getKeyAt(index) != key {
		// Thank you Mario! But our key is in another castle!
		return 0, false
	}
	entry := node.getCell(index)
	return entry.GetValue(), true
}

// keyToNodeEntry is a helper function to create cursors that point to a given index within a leaf node.
func (node *LeafNode) keyToNodeEntry(key int64) (*LeafNode, int64, error) {
	return node, node.search(key), nil
}

// printNode pretty prints our leaf node.
func (node *LeafNode) printNode(w io.Writer, firstPrefix string, prefix string) {
	// Format header data.
	var nodeType string = "Leaf"
	var isRoot string
	if node.isRoot() {
		isRoot = " (root)"
	}
	numKeys := strconv.Itoa(int(node.numKeys))
	// Print header data.
	io.WriteString(w, fmt.Sprintf("%v[%v] %v%v size: %v\n",
		firstPrefix, node.page.GetPageNum(), nodeType, isRoot, numKeys))
	// Print entries.
	for cellnum := int64(0); cellnum < node.numKeys; cellnum++ {
		entry := node.getCell(cellnum)
		io.WriteString(w, fmt.Sprintf("%v |--> (%v, %v)\n",
			prefix, entry.GetKey(), entry.GetValue()))
	}
	if node.rightSiblingPN > 0 {
		io.WriteString(w, fmt.Sprintf("%v |--+\n", prefix))
		io.WriteString(w, fmt.Sprintf("%v    | node @ %v\n",
			prefix, node.rightSiblingPN))
		io.WriteString(w, fmt.Sprintf("%v    v\n", prefix))
	}
}

// // printNode pretty prints our leaf node.
// func (node *LeafNode) printNode(w io.Writer, firstPrefix string, prefix string) {
// 	// Format header data.
// 	var nodeType string = "Leaf"
// 	var isRoot string
// 	if node.isRoot() {
// 		isRoot = " (root)"
// 	}
// 	numKeys := strconv.Itoa(int(node.numKeys))
// 	// Print header data.
// 	io.WriteString(w, fmt.Sprintf("%v[%v] %v%v size: %v\n",
// 		firstPrefix, node.page.GetPageNum(), nodeType, isRoot, numKeys))
// 	// Print entries.
// 	for cellnum := int64(0); cellnum < node.numKeys; cellnum++ {
// 		entry := node.getCell(cellnum)
// 		io.WriteString(w, fmt.Sprintf("%v |--> (%v, %v)\n",
// 			prefix, entry.GetKey(), entry.GetValue()))
// 	}
// 	if node.rightSiblingPN > 0 {
// 		io.WriteString(w, fmt.Sprintf("%v |--+\n", prefix))
// 		io.WriteString(w, fmt.Sprintf("%v    | right sibling @ [%v]\n",
// 			prefix, node.rightSiblingPN))
// 		io.WriteString(w, fmt.Sprintf("%v    v\n", prefix))
// 	}
// }

/////////////////////////////////////////////////////////////////////////////
/////////////////////////// Internal Node Methods ///////////////////////////
/////////////////////////////////////////////////////////////////////////////

// search returns the first index where key > given key.
// If no such index exists, it returns numKeys.
func (node *InternalNode) search(key int64) int64 {
	// search the nodes
	ind := sort.Search(int(node.numKeys), func(index int) bool { 
		return node.getKeyAt(int64(index)) > key 
	})
	return int64(ind)
}

// insert finds the appropriate place in a leaf node to insert a new tuple.
func (node *InternalNode) insert(key int64, value int64, update bool) Split {
	node.unlockParent(false)
	defer node.unlock()
	index := node.search(key)
	child, err := node.getChildAt(index, true)
	if err != nil {
		return Split{err: errors.New("node/insert internal: get child error")}
	}
	node.initChild(child)
	defer child.getPage().Put()
	split_check := child.insert(key, value, update)
	if split_check.isSplit {
		// node.unlock()
		// defer node.unlockParent(true)
		split_check = node.insertSplit(split_check)
	} 
	// else {
	// 	// defer node.unlockParent(true)
	// 	node.unlockParent(true)
	// }
	return split_check
}

// insertSplit inserts a split result into an internal node.
// If this insertion results in another split, the split is cascaded upwards.
func (node *InternalNode) insertSplit(split Split) Split {
	// search key position
	key := split.key
	key_pos := node.search(key)
	left_pn := split.leftPN
	right_pn := split.rightPN
	// shift key and pn to the right after key
	num_key := node.numKeys
	// update the last page
	last_page := node.getPNAt(num_key)
	node.updatePNAt(num_key+1, last_page)
	// update rest pairs
	for i:=num_key-1; i>=key_pos; i-- {
		key_val := node.getKeyAt(i)
		val_val := node.getPNAt(i)
		node.updateKeyAt(i+1, key_val)
		node.updatePNAt(i+1, val_val)
	}
	// pagenum update keys and pn at key position
	node.updateKeyAt(key_pos, key)
	node.updatePNAt(key_pos, left_pn)
	node.updatePNAt(key_pos+1, right_pn)
	node.updateNumKeys(num_key+1)

	// check split
	if node.numKeys > KEYS_PER_INTERNAL_NODE {
		res := node.split() 
		return res
	} else{
		return Split{isSplit: false}
	}
}

// delete removes a given tuple from the leaf node, if the given key exists.
func (node *InternalNode) delete(key int64) {
	node.unlockParent(true)
	index := node.search(key)
	child, err := node.getChildAt(index, true)
	if err != nil {
		return
	}
	defer child.getPage().Put()
	node.initChild(child)
	child.delete(key)
} 

// split is a helper function that splits an internal node, then propagates the split upwards.
func (node *InternalNode) split() Split {
	res := Split{}
	res.isSplit = true
	res.key = node.getKeyAt(node.numKeys/2)
	num_key := node.numKeys
	newNode, err := createInternalNode(node.page.GetPager())
	defer newNode.page.Put()
	if err != nil {
		res.err = errors.New("node/split: internal, fail to split node")
		return res
	}
	// shift to newnode
	for i:=num_key/2+1; i<num_key; i++ {
		cur_PN := node.getPNAt(i)
		cur_key := node.getKeyAt(i)
		newNode.updateKeyAt(newNode.numKeys, cur_key)
		newNode.updatePNAt(newNode.numKeys, cur_PN)
		newNode.updateNumKeys(newNode.numKeys+1)
	}
	// change last pagenum
	last_page := node.getPNAt(num_key)
	newNode.updatePNAt(newNode.numKeys, last_page)
	// change node numKeys instead of delete the node
	node.updateNumKeys(num_key/2)

	right_page_num := newNode.page.GetPageNum()
	res.rightPN = right_page_num
	left_page_num := node.page.GetPageNum()
	res.leftPN = left_page_num

	return res
}

// get returns the value associated with a given key from the leaf node.
func (node *InternalNode) get(key int64) (value int64, found bool) {
	node.unlockParent(true)
	childIdx := node.search(key)
	child, err := node.getChildAt(childIdx, true)
	if err != nil {
		// ??? unlock the parent, if err, do we need to re-lock the parent, if so, how
		return 0, false
	}
	// the gearup says the function is lockchild, but not exist, initchild instead?
	node.initChild(child)
	defer child.getPage().Put()
	return child.get(key)
}

// keyToNodeEntry is a helper function to create cursors that point to a given index within a leaf node.
func (node *InternalNode) keyToNodeEntry(key int64) (*LeafNode, int64, error) {
	index := node.search(key)
	child, err := node.getChildAt(index, false)
	if err != nil {
		return &LeafNode{}, 0, err
	}
	defer child.getPage().Put()
	return child.keyToNodeEntry(key)
}

// printNode pretty prints our internal node.
func (node *InternalNode) printNode(w io.Writer, firstPrefix string, prefix string) {
	// Format header data.
	var nodeType string = "Internal"
	var isRoot string
	if node.isRoot() {
		isRoot = " (root)"
	}
	numKeys := strconv.Itoa(int(node.numKeys + 1))
	// Print header data.
	io.WriteString(w, fmt.Sprintf("%v[%v] %v%v size: %v\n",
		firstPrefix, node.page.GetPageNum(), nodeType, isRoot, numKeys))
	// Print entries.
	nextFirstPrefix := prefix + " |--> "
	nextPrefix := prefix + " |    "
	for idx := int64(0); idx <= node.numKeys; idx++ {
		io.WriteString(w, fmt.Sprintf("%v\n", nextPrefix))
		child, err := node.getChildAt(idx, false)
		if err != nil {
			return
		}
		defer child.getPage().Put()
		child.printNode(w, nextFirstPrefix, nextPrefix)
	}
}
// // printNode pretty prints our internal node.
// func (node *InternalNode) printNode(w io.Writer, firstPrefix string, prefix string) {
// 	// Format header data.
// 	var nodeType string = "Internal"
// 	var isRoot string
// 	if node.isRoot() {
// 		isRoot = " (root)"
// 	}
// 	numKeys := strconv.Itoa(int(node.numKeys + 1))
// 	// Print header data.
// 	io.WriteString(w, fmt.Sprintf("%v[%v] %v%v size: %v\n",
// 		firstPrefix, node.page.GetPageNum(), nodeType, isRoot, numKeys))
// 	// Print entries.
// 	nextFirstPrefix := prefix + " |--> "
// 	nextPrefix := prefix + " |    "
// 	for idx := int64(0); idx <= node.numKeys; idx++ {
// 		io.WriteString(w, fmt.Sprintf("%v\n", nextPrefix))
// 		child, err := node.getChildAt(idx)
// 		if err != nil {
// 			return
// 		}
// 		defer child.getPage().Put()
// 		child.printNode(w, nextFirstPrefix, nextPrefix)
// 		if idx != node.numKeys {
// 			io.WriteString(w, fmt.Sprintf("\n%v[KEY] %v\n", nextPrefix, node.getKeyAt(idx)))
// 		}
// 	}
// }