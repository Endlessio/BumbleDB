package recovery

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	concurrency "github.com/brown-csci1270/db/pkg/concurrency"
	db "github.com/brown-csci1270/db/pkg/db"
	"github.com/otiai10/copy"

	uuid "github.com/google/uuid"
)

// Recovery Manager.
type RecoveryManager struct {
	d       *db.Database
	tm      *concurrency.TransactionManager
	txStack map[uuid.UUID]([]Log)
	fd      *os.File
	mtx     sync.Mutex
}

// Construct a recovery manager.
func NewRecoveryManager(
	d *db.Database,
	tm *concurrency.TransactionManager,
	logName string,
) (*RecoveryManager, error) {
	fd, err := os.OpenFile(logName, os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	return &RecoveryManager{
		d:       d,
		tm:      tm,
		txStack: make(map[uuid.UUID][]Log),
		fd:      fd,
	}, nil
}

// Write the string `s` to the log file. Expects rm.mtx to be locked
func (rm *RecoveryManager) writeToBuffer(s string) error {
	_, err := rm.fd.WriteString(s)
	if err != nil {
		return err
	}
	err = rm.fd.Sync()
	return err
}

// Write a Table log.
func (rm *RecoveryManager) Table(tblType string, tblName string) {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()
	// panic("function not yet implemented");
	new_table_log := tableLog{
		tblType: tblType,
		tblName: tblName}
	rm.writeToBuffer(new_table_log.toString())
}

// Write an Edit log.
func (rm *RecoveryManager) Edit(clientId uuid.UUID, table db.Index, action Action, key int64, oldval int64, newval int64) {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()
	new_edit_log := editLog{
		id:        clientId,
		tablename: table.GetName(),
		action:    action,
		key:       key,
		oldval:    oldval,
		newval:    newval}

	rm.txStack[clientId] = append(rm.txStack[clientId], &new_edit_log)

	rm.writeToBuffer(new_edit_log.toString())
}

// Write a transaction start log.
func (rm *RecoveryManager) Start(clientId uuid.UUID) {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()
	// panic("function not yet implemented");
	new_txn := make([]Log, 0)
	rm.txStack[clientId] = new_txn
	new_start_log := startLog{
		id: clientId}

	rm.txStack[clientId] = append(rm.txStack[clientId], &new_start_log)

	rm.writeToBuffer(new_start_log.toString())
}

// Write a transaction commit log.
func (rm *RecoveryManager) Commit(clientId uuid.UUID) {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()
	// panic("function not yet implemented");
	new_commit_log := commitLog{
		id: clientId}
	delete(rm.txStack, clientId)
	rm.writeToBuffer(new_commit_log.toString())
}

// Flush all pages to disk and write a checkpoint log.
func (rm *RecoveryManager) Checkpoint() {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()
	// panic("function not yet implemented");
	new_id_list := make([]uuid.UUID, 0)
	table_list := rm.d.GetTables()
	for _, table := range table_list {
		table.GetPager().LockAllUpdates()
		table.GetPager().FlushAllPages()
		table.GetPager().UnlockAllUpdates()
	}
	for _,tx := range rm.tm.GetTransactions() {
		new_id_list = append(new_id_list, tx.GetClientID())
	}
	new_check_log := checkpointLog{
		ids: new_id_list}
	rm.writeToBuffer(new_check_log.toString())
	rm.Delta() // Sorta-semi-pseudo-copy-on-write (to ensure db recoverability)
}

// Redo a given log's action.
func (rm *RecoveryManager) Redo(log Log) error {
	switch log := log.(type) {
	case *tableLog:
		payload := fmt.Sprintf("create %s table %s", log.tblType, log.tblName)
		err := db.HandleCreateTable(rm.d, payload, os.Stdout)
		if err != nil {
			return err
		}
	case *editLog:
		switch log.action {
		case INSERT_ACTION:
			payload := fmt.Sprintf("insert %v %v into %s", log.key, log.newval, log.tablename)
			err := db.HandleInsert(rm.d, payload)
			if err != nil {
				// There is already an entry, try updating
				payload := fmt.Sprintf("update %s %v %v", log.tablename, log.key, log.newval)
				err = db.HandleUpdate(rm.d, payload)
				if err != nil {
					return err
				}
			}
		case UPDATE_ACTION:
			payload := fmt.Sprintf("update %s %v %v", log.tablename, log.key, log.newval)
			err := db.HandleUpdate(rm.d, payload)
			if err != nil {
				// Entry may have been deleted, try inserting
				payload := fmt.Sprintf("insert %v %v into %s", log.key, log.newval, log.tablename)
				err := db.HandleInsert(rm.d, payload)
				if err != nil {
					return err
				}
			}
		case DELETE_ACTION:
			payload := fmt.Sprintf("delete %v from %s", log.key, log.tablename)
			err := db.HandleDelete(rm.d, payload)
			if err != nil {
				return err
			}
		}
	default:
		return errors.New("can only redo edit logs")
	}
	return nil
}

// Undo a given log's action.
func (rm *RecoveryManager) Undo(log Log) error {
	switch log := log.(type) {
	case *editLog:
		switch log.action {
		case INSERT_ACTION:
			payload := fmt.Sprintf("delete %v from %s", log.key, log.tablename)
			err := HandleDelete(rm.d, rm.tm, rm, payload, log.id)
			if err != nil {
				return err
			}
		case UPDATE_ACTION:
			payload := fmt.Sprintf("update %s %v %v", log.tablename, log.key, log.oldval)
			err := HandleUpdate(rm.d, rm.tm, rm, payload, log.id)
			if err != nil {
				return err
			}
		case DELETE_ACTION:
			payload := fmt.Sprintf("insert %v %v into %s", log.key, log.oldval, log.tablename)
			err := HandleInsert(rm.d, rm.tm, rm, payload, log.id)
			if err != nil {
				return err
			}
		}
	default:
		return errors.New("can only undo edit logs")
	}
	return nil
}

// Do a full recovery to the most recent checkpoint on startup.
func (rm *RecoveryManager) Recover() error {
	log_list, check_pos, err := rm.readLogs()
	fmt.Println("log list len, check_pos:", len(log_list), check_pos)
	if err != nil {
		return errors.New("recovery/Recovery: err from readLogs")
	}
	// invalid check point, return err/nil?
	if check_pos >= len(log_list) {
		return nil
	}

	// get active txn: read through transaction stack
	// active_txn, ok := log_list[check_pos].(*checkpointLog)
	// active_map := make(map[uuid.UUID]bool)
	// if ok {
	// 	for _, ele := range active_txn.ids {
	// 		active_map[ele] = true
	// 		err := rm.tm.Begin(ele)
	// 		if err != nil {
	// 			return err
	// 		}
	// 	}
	// }
	active_txn:= log_list[check_pos].(*checkpointLog).ids
	active_map := make(map[uuid.UUID]bool)
	for _, ele := range active_txn {
		active_map[ele] = true
		err := rm.tm.Begin(ele)
		if err != nil {
			return err
		}
	


	// redo part
	for i := check_pos; i < len(log_list); i++ {
		log := log_list[i]
		switch cur_log := log.(type) {
		case *commitLog:
			txn_id := cur_log.id
			delete(active_map, txn_id)
			err = rm.tm.Commit(txn_id)
			if err != nil {
				return err
			}
		case *startLog:
			txn_id := cur_log.id
			// add to begin also active_txn
			active_map[txn_id] = true
			err := rm.tm.Begin(txn_id)
			if err != nil {
				return err
			}
		case *tableLog:
			err := rm.Redo(cur_log)
			if err != nil {
				return err
			}
		case *editLog:
			err := rm.Redo(cur_log)
			if err != nil {
				return err
			}
		default:
			continue
		}
	}
	// undo part uncommitted txns
	for i := len(log_list) - 1; i >= 0; i-- {
		if len(active_map) == 0 {
			break
		}
		log := log_list[i]
		switch cur_log := log.(type) {
		case *editLog:
			txn_id := cur_log.id
			if _, ok := active_map[txn_id]; ok {
				err := rm.Undo(cur_log)
				if err != nil {
					return err
				}
			}
		case *startLog:
			txn_id := cur_log.id
			if _, ok := active_map[txn_id]; ok {
				rm.Commit(txn_id)
				err = rm.tm.Commit(txn_id)
				if err != nil {
					return err
				}
				// rm active
				delete(active_map, txn_id)
			}
		default:
			continue
		}
	}
	return nil
}

// Roll back a particular transaction.
func (rm *RecoveryManager) Rollback(clientId uuid.UUID) error {
	// panic("function not yet implemented");
	txn_list, ok := rm.txStack[clientId]
	if !ok {
		return errors.New("recovery/Rollback: no target txn")
	}
	// check zero:
	if len(txn_list) == 0 {
		rm.Commit(clientId)
		rm.tm.Commit(clientId)
		return nil
	}
	// check invalid: first is not start
	first := txn_list[0]
	switch first.(type) {
	case *startLog:
	default:
		return errors.New("recovery/Rollback: invalid logs")
	}
	// just rollback
	for i := len(txn_list) - 1; i > 0; i-- {
		cur_log := txn_list[i]
		err := rm.Undo(cur_log)
		if err != nil {
			return err
		}
	}
	rm.Commit(clientId)
	rm.tm.Commit(clientId)

	return nil
}

// Primes the database for recovery
func Prime(folder string) (*db.Database, error) {
	// Ensure folder is of the form */
	base := strings.TrimSuffix(folder, "/")
	recoveryFolder := base + "-recovery/"
	dbFolder := base + "/"
	if _, err := os.Stat(dbFolder); err != nil {
		if os.IsNotExist(err) {
			err := os.MkdirAll(recoveryFolder, 0775)
			if err != nil {
				return nil, err
			}
			return db.Open(dbFolder)
		}
		return nil, err
	}
	if _, err := os.Stat(recoveryFolder); err != nil {
		if os.IsNotExist(err) {
			return db.Open(dbFolder)
		}
		return nil, err
	}
	os.RemoveAll(dbFolder)
	err := copy.Copy(recoveryFolder, dbFolder)
	if err != nil {
		return nil, err
	}
	return db.Open(dbFolder)
}

// Should be called at end of Checkpoint.
func (rm *RecoveryManager) Delta() error {
	folder := strings.TrimSuffix(rm.d.GetBasePath(), "/")
	recoveryFolder := folder + "-recovery/"
	folder += "/"
	os.RemoveAll(recoveryFolder)
	err := copy.Copy(folder, recoveryFolder)
	return err
}
