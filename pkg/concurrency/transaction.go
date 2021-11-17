package concurrency

import (
	"errors"
	"sync"

	db "github.com/brown-csci1270/db/pkg/db"
	uuid "github.com/google/uuid"
)

// Each client can have a transaction running. Each transaction has a list of locked resources.
type Transaction struct {
	clientId  uuid.UUID
	resources map[Resource]LockType
	lock      sync.RWMutex
}

// Grab a write lock on the tx
func (t *Transaction) WLock() {
	t.lock.Lock()
}

// Release the write lock on the tx
func (t *Transaction) WUnlock() {
	t.lock.Unlock()
}

// Grab a read lock on the tx
func (t *Transaction) RLock() {
	t.lock.RLock()
}

// Release the write lock on the tx
func (t *Transaction) RUnlock() {
	t.lock.RUnlock()
}

// Get the transaction id.
func (t *Transaction) GetClientID() uuid.UUID {
	return t.clientId
}

// Get the transaction's resources.
func (t *Transaction) GetResources() map[Resource]LockType {
	return t.resources
}

// Transaction Manager manages all of the transactions on a server.
type TransactionManager struct {
	lm           *LockManager
	tmMtx        sync.RWMutex
	pGraph       *Graph
	transactions map[uuid.UUID]*Transaction
}

// Get a pointer to a new transaction manager.
func NewTransactionManager(lm *LockManager) *TransactionManager {
	return &TransactionManager{lm: lm, pGraph: NewGraph(), transactions: make(map[uuid.UUID]*Transaction)}
}

// Get the transactions.
func (tm *TransactionManager) GetLockManager() *LockManager {
	return tm.lm
}

// Get the transactions.
func (tm *TransactionManager) GetTransactions() map[uuid.UUID]*Transaction {
	return tm.transactions
}

// Get a particular transaction.
func (tm *TransactionManager) GetTransaction(clientId uuid.UUID) (*Transaction, bool) {
	tm.tmMtx.RLock()
	defer tm.tmMtx.RUnlock()
	t, found := tm.transactions[clientId]
	return t, found
}

// Begin a transaction for the given client; error if already began.
func (tm *TransactionManager) Begin(clientId uuid.UUID) error {
	tm.tmMtx.Lock()
	defer tm.tmMtx.Unlock()
	_, found := tm.transactions[clientId]
	if found {
		return errors.New("transaction already began")
	}
	tm.transactions[clientId] = &Transaction{clientId: clientId, resources: make(map[Resource]LockType)}
	return nil
}

// Locks the given resource. Will return an error if deadlock is created.
func (tm *TransactionManager) Lock(clientId uuid.UUID, table db.Index, resourceKey int64, lType LockType) error {
	tm.tmMtx.RLock()
	cur_tran, ok := tm.GetTransaction(clientId)
	if !ok {
		return errors.New("concurrency/transaction: clientId not find")
	}
	cur_tran.RLock()
	resrc_list := cur_tran.GetResources()
	// Check if the transaction has rights to the resource it is trying to lock
	old_lock, exist := resrc_list[Resource{table.GetName(), resourceKey}]
	if exist {
		if old_lock == R_LOCK && lType == W_LOCK {
			return errors.New("concurrency/transaction/lock: old read, new write")
		}
	}
	cur_tran.RUnlock()
	// Look for other transactions that might conflict with our transaction
	conflict_list := tm.discoverTransactions(Resource{table.GetName(), resourceKey}, lType)
	// If a conflicting transaction is found, add an edge to the precedence graph
	for i:=0; i<len(conflict_list); i++ {
		cur_conflict := conflict_list[i]
		if cur_tran != cur_conflict {
			tm.pGraph.AddEdge(cur_tran, cur_conflict)
			defer tm.pGraph.RemoveEdge(cur_tran, cur_conflict)
		}
	}
	// Check for deadlocks in the precedence graph
	deadlock := tm.pGraph.DetectCycle()
	if deadlock {
		tm.tmMtx.RUnlock()
		return errors.New("concurrency/transaction/lock: deadlock")
	}
	// Add the resource to the transaction’s resource list and lock it
	err := tm.lm.Lock(Resource{table.GetName(), resourceKey}, lType)
	if err != nil {
		return err
	}
	tm.tmMtx.RUnlock()
	cur_tran.WLock()
	resrc_list[Resource{table.GetName(), resourceKey}] = lType
	cur_tran.WUnlock()
	return nil
}

// Unlocks the given resource.
func (tm *TransactionManager) Unlock(clientId uuid.UUID, table db.Index, resourceKey int64, lType LockType) error {
	tm.tmMtx.RLock()
	cur_tran, ok := tm.GetTransaction(clientId)
	if !ok {
		tm.tmMtx.RUnlock()
		return errors.New("concurrency/transaction/unlock: clientId not find")
	}
	cur_tran.RLock()
	resrc_list := cur_tran.GetResources()
	// find the transaction
	old_lock, exist := resrc_list[Resource{table.GetName(), resourceKey}]
	cur_tran.RUnlock()
	if exist {
		if old_lock == lType {
			err := tm.lm.Unlock(Resource{table.GetName(), resourceKey}, lType)
			if err != nil {
				return err
			}
			tm.tmMtx.RUnlock()
			cur_tran.WLock()
			delete(resrc_list, Resource{table.GetName(), resourceKey})
			cur_tran.WUnlock()
		} else {
			tm.tmMtx.RUnlock()
			return errors.New("concurrency/transaction/unlock: lock type not match")
		}
	} else {
		tm.tmMtx.RUnlock()
		return errors.New("concurrency/transaction/unlock: the resource not find")
	}
	return nil
}

// Commits the given transaction and removes it from the running transactions list.
func (tm *TransactionManager) Commit(clientId uuid.UUID) error {
	tm.tmMtx.Lock()
	defer tm.tmMtx.Unlock()
	// Get the transaction we want.
	t, found := tm.transactions[clientId]
	if !found {
		return errors.New("no transactions running")
	}
	// Unlock all resources.
	t.RLock()
	defer t.RUnlock()
	for r, lType := range t.resources {
		err := tm.lm.Unlock(r, lType)
		if err != nil {
			return err
		}
	}
	// Remove the transaction from our transactions list.
	delete(tm.transactions, clientId)
	return nil
}

// Returns a slice of all transactions that conflict w/ the given resource and locktype.
func (tm *TransactionManager) discoverTransactions(r Resource, lType LockType) []*Transaction {
	ret := make([]*Transaction, 0)
	for _, t := range tm.transactions {
		t.RLock()
		for storedResource, storedType := range t.resources {
			if storedResource == r && (storedType == W_LOCK || lType == W_LOCK) {
				ret = append(ret, t)
				break
			}
		}
		t.RUnlock()
	}
	return ret
}
