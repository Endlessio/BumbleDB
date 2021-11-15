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
	panic("function not yet implemented");
}

// Finds the top-most parent of `t`.
func find(parent []int, t int) int {
	if parent[t] == -1 {
		return t
	}
	return find(parent, parent[t])
}

// Unions the sets that `t1` and `t2` ar ein. Returns true if the two are the same set.
func union(parent []int, t1 int, t2 int) ([]int, bool) {
	p1 := find(parent, t1)
	p2 := find(parent, t2)
	parent[t1] = p2
	return parent, p1 == p2
}

// Gets the index of `t` in the parent array.
func getIndex(transactions []*Transaction, t *Transaction) int {
	for i, x := range transactions {
		if x == t {
			return i
		}
	}
	return -1
}

// Remove the element at index `i` from `l`.
func removeEdge(l []Edge, i int) []Edge {
	l[i] = l[len(l)-1]
	return l[:len(l)-1]
}
