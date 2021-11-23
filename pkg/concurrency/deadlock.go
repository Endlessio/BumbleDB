package concurrency

import (
	"errors"
	"sync"
)

// Graph.
type Graph struct {
	edges []Edge
	lock  sync.RWMutex
}

// Edge.
type Edge struct {
	from *Transaction
	to   *Transaction
}

// Grab a write lock on the graph
func (g *Graph) WLock() {
	g.lock.Lock()
}

// Release the write lock on the graph
func (g *Graph) WUnlock() {
	g.lock.Unlock()
}

// Grab a read lock on the graph
func (g *Graph) RLock() {
	g.lock.RLock()
}

// Release the write lock on the graph
func (g *Graph) RUnlock() {
	g.lock.RUnlock()
}

// Construct a new graph.
func NewGraph() *Graph {
	return &Graph{edges: make([]Edge, 0)}
}

// Add an edge from `from` to `to`. Logically, `from` waits for `to`.
func (g *Graph) AddEdge(from *Transaction, to *Transaction) {
	g.WLock()
	defer g.WUnlock()
	g.edges = append(g.edges, Edge{from: from, to: to})
}

// Remove an edge. Only removes one of these edges if multiple copies exist.
func (g *Graph) RemoveEdge(from *Transaction, to *Transaction) error {
	g.WLock()
	defer g.WUnlock()
	toRemove := Edge{from: from, to: to}
	for i, e := range g.edges {
		if e == toRemove {
			g.edges = removeEdge(g.edges, i)
			return nil
		}
	}
	return errors.New("edge not found")
}

// Return true if a cycle exists; false otherwise.
func (g *Graph) DetectCycle() bool {
	g.RLock()
	defer g.RUnlock()
	seen := make([]*Transaction, 0)
	for _, edge := range g.edges {
		key := edge.from
		if dfs(g,key,seen) {
			return true
		}
	}
	return false
	// g.RLock()
	// defer g.RUnlock()
	// edges := g.edges
	// length := len(edges)
	// trans := map[*Transaction]bool{}
	// trans_list := make([]*Transaction, 0)
	// parent_list := make([]int, 0)

	// for i:=0; i<length; i++ {
	// 	cur_edge := edges[i]
	// 	_, ok1 := trans[cur_edge.from]
	// 	_, ok2 := trans[cur_edge.to]
	// 	if !ok1 {
	// 		trans[cur_edge.from] = true
	// 		trans_list = append(trans_list, cur_edge.from)
	// 		parent_list = append(parent_list, -1)
	// 	}
	// 	if !ok2 {
	// 		trans[cur_edge.to] = true
	// 		trans_list = append(trans_list, cur_edge.to)
	// 		parent_list = append(parent_list, -1)
	// 	}
	// 	from_tran := getIndex(trans_list, cur_edge.from)
	// 	to_tran := getIndex(trans_list, cur_edge.to)
	// 	_, exist := union(parent_list, from_tran, to_tran)
	// 	if exist {
	// 		return true
	// 	} 
	// }
	// return false
}


func dfs(g *Graph, from *Transaction, seen []*Transaction) bool {
	// Go through each edge.
	for _, e := range g.edges {
		// If there is an edge from here to elsewhere,
		if e.from == from {
			// Check if it creates a cycle.
			for _, s := range seen {
				if e.to == s {
					return true
				}
			}
			// Otherwise, run dfs on it.
			return dfs(g, e.to, append(seen, e.from))
		}
	}
	return false
}

// Remove the element at index `i` from `l`.
func removeEdge(l []Edge, i int) []Edge {
	l[i] = l[len(l)-1]
	return l[:len(l)-1]
}
