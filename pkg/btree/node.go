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
	idx := node.search(key)

	if update && idx == node.numKeys {
		return Split{err: errors.New("node/insertleaf: update non exist")}
	}
	
	if idx < node.numKeys && node.getKeyAt(idx) == key {
		if update {
			node.updateValueAt(idx, value)
			return Split{}
		} else {
			return Split{err: errors.New("node/insertleaf: duplicated but not update")}
		}
	} else {
		if update {
			// fmt.Println("yes")
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
		return res
	} else {
		return Split{isSplit: false}
	}
	// // deal with the zero case
	// if idx == 0 && node.numKeys == 0 && key == 0 {
	// 	node.updateKeyAt(idx, key)
	// 	node.updateValueAt(idx, value)
	// 	// update number of keys
	// 	node.updateNumKeys(node.numKeys+1)
	// } else {
	// 	// larger than all of the keys: should be inserted at the last 
	// 	if idx == node.numKeys {
	// 		// at this time, if update, it would be update non-exist, return error
	// 		if update {
	// 			return Split{err: errors.New("node/insertleaf: update not exist")}
	// 		}
	// 		// insert at last
	// 		node.updateKeyAt(idx, key)
	// 		node.updateValueAt(idx, value)
	// 		// update number of keys
	// 		node.updateNumKeys(node.numKeys+1)
	// 	// insert in the middle
	// 	} else if idx < node.numKeys {
	// 		// if duplicated
	// 		if node.getKeyAt(idx) == key {
	// 			// if update: update
	// 			if update {
	// 				node.updateValueAt(idx, value)
	// 				return Split{}
	// 			// if duplicated but not update: error
	// 			} else {
	// 				return Split{err: errors.New("node/insertleaf: duplicated but not update")}
	// 			}
	// 		// if not duplicated, normal insert
	// 		} else {
	// 			for i:=node.numKeys-1; i>=idx; i-- {
	// 				key_val := node.getKeyAt(i)
	// 				val_val := node.getValueAt(i)
	// 				node.updateKeyAt(i+1, key_val)
	// 				node.updateValueAt(i+1, val_val)
	// 			}
	// 			node.updateKeyAt(idx, key)
	// 			node.updateValueAt(idx, value)
	// 			// update number of keys
	// 			node.updateNumKeys(node.numKeys+1)
	// 		}
	// 	}
	// }
	// check split or not

	// if node.getKeyAt(idx) == key {
	// 	if idx < node.numKeys {
	// 		if update {
	// 			node.updateValueAt(idx, value)
	// 			return Split{}
	// 		} else {
	// 			// fmt.Println("the key", node.getKeyAt(idx), key, idx)
	// 			return Split{err: errors.New("node/insertleaf: duplicated but not update")}
	// 		}
	// 	} else if idx == node.numKeys {
	// 		if update {
	// 			return Split{err: errors.New("node/insertleaf: update not exist")}
	// 		} else {
	// 			if idx == 0 {
	// 				node.updateKeyAt(idx, key)
	// 				node.updateValueAt(idx, value)
	// 				// update number of keys
	// 				node.updateNumKeys(node.numKeys+1)
	// 			} else {
	// 				return Split{err: errors.New("node/insertleaf: duplicated but not update")}
	// 			}
	// 		}
	// 	}
	// }
	// if idx == node.numKeys && idx != 0{
	// 	node.updateKeyAt(idx, key)
	// 	node.updateValueAt(idx, value)
	// 	// update number of keys
	// 	node.updateNumKeys(node.numKeys+1)
	// }else if idx < node.numKeys {
	// 	// move (idx i) to (idx i+1)
	// 	for i:=node.numKeys-1; i>=idx; i-- {
	// 		// fmt.Println("done")
	// 		key_val := node.getKeyAt(i)
	// 		val_val := node.getValueAt(i)
	// 		node.updateKeyAt(i+1, key_val)
	// 		node.updateValueAt(i+1, val_val)
	// 	}
	// 	// update the key, value of insert tuple at the searched index
	// 	// node.modifyCell(idx, BTre eEntry{key: key, value: value})
	// 	node.updateKeyAt(idx, key)
	// 	node.updateValueAt(idx, value)
	// 	// update number of keys
	// 	node.updateNumKeys(node.numKeys+1)
	// }
	// fmt.Println("index", key, idx, node.numKeys, node.getKeyAt(0), node.getKeyAt(1), node.getKeyAt(2),node.getKeyAt(3))
}

// delete removes a given tuple from the leaf node, if the given key exists.
func (node *LeafNode) delete(key int64) {
	ind := node.search(key)
	// exist 
	if ind < node.numKeys {
		for i:=int64(ind); i<=int64(node.numKeys)-2; i++{
			key_val:=node.getKeyAt(i+1)
			val_val:=node.getValueAt(i+1)
			node.updateKeyAt(i, key_val)
			node.updateValueAt(i, val_val)
			// node.modifyCell(i, BTreeEntry{key: key_val, value: val_val})
		}
		node.updateNumKeys(node.numKeys-1)
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
		return res
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
	// delete part of the original node data
	node.updateNumKeys(node.numKeys/2)

	right_page_num := newNode.page.GetPageNum()
	res.rightPN = right_page_num
	left_page_num := node.page.GetPageNum()
	res.leftPN = left_page_num
	return res
}

// get returns the value associated with a given key from the leaf node.
func (node *LeafNode) get(key int64) (value int64, found bool) {
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
	index := node.search(key)
	child, err := node.getChildAt(index)
	if err != nil {
		return Split{err: errors.New("node/insert internal: get child error")}
	}
	defer child.getPage().Put()
	split_check := child.insert(key, value, update)
	// update the key number
	// node.updateNumKeys(node.numKeys+1)
	if split_check.isSplit {
		res := node.insertSplit(split_check)
		return res
	} else {
		return Split{isSplit: false}
	}
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
	index := node.search(key)
	child, err := node.getChildAt(index)
	if err == nil {
		child.delete(key)
	}
	defer child.getPage().Put()
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
	for i:=num_key/2; i<num_key; i++ {
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
	childIdx := node.search(key)
	child, err := node.getChildAt(childIdx)
	if err != nil {
		return 0, false
	}
	defer child.getPage().Put()
	return child.get(key)
}

// keyToNodeEntry is a helper function to create cursors that point to a given index within a leaf node.
func (node *InternalNode) keyToNodeEntry(key int64) (*LeafNode, int64, error) {
	index := node.search(key)
	child, err := node.getChildAt(index)
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
		child, err := node.getChildAt(idx)
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