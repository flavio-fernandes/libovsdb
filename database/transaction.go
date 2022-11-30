package database

import (
	"fmt"
	"reflect"
	"time"

	"github.com/go-logr/logr"
	"github.com/google/uuid"
	"github.com/ovn-org/libovsdb/cache"
	"github.com/ovn-org/libovsdb/model"
	"github.com/ovn-org/libovsdb/ovsdb"
	"github.com/ovn-org/libovsdb/updates"
)

type Transaction struct {
	ID          uuid.UUID
	Cache       *cache.TableCache
	DeletedRows map[string]struct{}
	Model       model.DatabaseModel
	DbName      string
	Database    Database
}

func NewTransaction(model model.DatabaseModel, dbName string, database Database, logger *logr.Logger) Transaction {
	if logger != nil {
		l := logger.WithName("transaction")
		logger = &l
	}
	cache, err := cache.NewTableCache(model, nil, logger)
	if err != nil {
		panic(err)
	}
	return Transaction{
		ID:          uuid.New(),
		Cache:       cache,
		DeletedRows: make(map[string]struct{}),
		Model:       model,
		DbName:      dbName,
		Database:    database,
	}
}

func (t *Transaction) Transact(operations []ovsdb.Operation) ([]*ovsdb.OperationResult, Update) {
	results := []*ovsdb.OperationResult{}
	update := updates.ModelUpdates{}

	var r ovsdb.OperationResult
	for _, op := range operations {
		// if we had a previous error, just append a nil result for every op
		// after that
		if r.Error != "" {
			results = append(results, nil)
			continue
		}

		// simple case: database name does not exist
		if !t.Database.Exists(t.DbName) {
			r = ovsdb.OperationResult{
				Error: "database does not exist",
			}
			results = append(results, &r)
			continue
		}

		var u *updates.ModelUpdates
		switch op.Op {
		case ovsdb.OperationInsert:
			r, u = t.Insert(&op)
		case ovsdb.OperationSelect:
			r = t.Select(op.Table, op.Where, op.Columns)
		case ovsdb.OperationUpdate:
			r, u = t.Update(&op)
		case ovsdb.OperationMutate:
			r, u = t.Mutate(&op)
		case ovsdb.OperationDelete:
			r, u = t.Delete(&op)
		case ovsdb.OperationWait:
			r = t.Wait(op.Table, op.Timeout, op.Where, op.Columns, op.Until, op.Rows)
		case ovsdb.OperationCommit:
			durable := op.Durable
			r = t.Commit(op.Table, *durable)
		case ovsdb.OperationAbort:
			r = t.Abort(op.Table)
		case ovsdb.OperationComment:
			r = t.Comment(op.Table, *op.Comment)
		case ovsdb.OperationAssert:
			r = t.Assert(op.Table, *op.Lock)
		default:
			r = ovsdb.ResultFromError(&ovsdb.NotSupported{})
		}

		if r.Error == "" && u != nil {
			err := update.Merge(t.Model, *u)
			if err != nil {
				r = ovsdb.ResultFromError(err)
			}
			if err := t.Cache.ApplyCacheUpdate(*u); err != nil {
				r = ovsdb.ResultFromError(err)
			}
			u = nil
		}

		result := r
		results = append(results, &result)
	}

	// if an operation failed, no need to do any further validation
	if r.Error != "" {
		return results, update
	}

	// check index constraints
	if err := t.checkIndexes(); err != nil {
		if indexExists, ok := err.(*cache.ErrIndexExists); ok {
			err = ovsdb.NewConstraintViolation(newIndexExistsDetails(*indexExists))
			r := ovsdb.ResultFromError(err)
			results = append(results, &r)
		} else {
			r := ovsdb.ResultFromError(err)
			results = append(results, &r)
		}
	}

	return results, update
}

func (t *Transaction) rowsFromTransactionCacheAndDatabase(table string, where []ovsdb.Condition) (map[string]model.Model, error) {
	txnRows, err := t.Cache.Table(table).RowsByCondition(where)
	if err != nil {
		return nil, fmt.Errorf("failed getting rows for table %s from transaction cache: %v", table, err)
	}
	rows, err := t.Database.List(t.DbName, table, where...)
	if err != nil {
		return nil, fmt.Errorf("failed getting rows for table %s from database: %v", table, err)
	}

	// prefer rows from transaction cache while copying into cache
	// rows that are in the db.
	for rowUUID, row := range rows {
		if txnRow, found := txnRows[rowUUID]; found {
			rows[rowUUID] = txnRow
			// delete txnRows so that only inserted rows remain in txnRows
			delete(txnRows, rowUUID)
		} else {
			// warm the transaction cache with the current contents of the row
			if err := t.Cache.Table(table).Create(rowUUID, row, false); err != nil {
				return nil, fmt.Errorf("failed warming transaction cache row %s %v for table %s: %v", rowUUID, row, table, err)
			}
		}
	}
	// add rows that have been inserted in this transaction
	for rowUUID, row := range txnRows {
		rows[rowUUID] = row
	}
	// exclude deleted rows
	for rowUUID := range t.DeletedRows {
		delete(rows, rowUUID)
	}
	return rows, nil
}

// checkIndexes checks that there are no index conflicts:
// - no duplicate indexes among any two rows operated with in the transaction
// - no duplicate indexes of any transaction row with any database row
func (t *Transaction) checkIndexes() error {
	// check for index conflicts.
	tables := t.Cache.Tables()
	for _, table := range tables {
		tc := t.Cache.Table(table)
		for _, row := range tc.RowsShallow() {
			err := tc.IndexExists(row)
			if err != nil {
				return err
			}
			err = t.Database.CheckIndexes(t.DbName, table, row)
			errIndexExists, isErrIndexExists := err.(*cache.ErrIndexExists)
			if err == nil {
				continue
			}
			if !isErrIndexExists {
				return err
			}
			for _, existing := range errIndexExists.Existing {
				if _, isDeleted := t.DeletedRows[existing]; isDeleted {
					// this model is deleted in the transaction, ignore it
					continue
				}
				if tc.HasRow(existing) {
					// this model is updated in the transaction and was not
					// detected as a duplicate, so an index must have been
					// updated, ignore it
					continue
				}
				return err
			}
		}
	}
	return nil
}

func (t *Transaction) Insert(op *ovsdb.Operation) (ovsdb.OperationResult, *updates.ModelUpdates) {
	rowUUID := op.UUIDName
	if rowUUID == "" {
		rowUUID = uuid.NewString()
	}

	update := updates.ModelUpdates{}
	err := update.AddOperation(t.Model, op.Table, rowUUID, nil, op)
	if err != nil {
		return ovsdb.ResultFromError(err), nil
	}

	result := ovsdb.OperationResult{
		UUID: ovsdb.UUID{GoUUID: rowUUID},
	}

	return result, &update
}

func (t *Transaction) Select(table string, where []ovsdb.Condition, columns []string) ovsdb.OperationResult {
	var results []ovsdb.Row
	dbModel := t.Model

	rows, err := t.rowsFromTransactionCacheAndDatabase(table, where)
	if err != nil {
		return ovsdb.ResultFromError(err)
	}

	m := dbModel.Mapper
	for _, row := range rows {
		info, err := dbModel.NewModelInfo(row)
		if err != nil {
			return ovsdb.ResultFromError(err)
		}
		resultRow, err := m.NewRow(info)
		if err != nil {
			return ovsdb.ResultFromError(err)
		}
		results = append(results, resultRow)
	}
	return ovsdb.OperationResult{
		Rows: results,
	}
}

func (t *Transaction) Update(op *ovsdb.Operation) (ovsdb.OperationResult, *updates.ModelUpdates) {
	rows, err := t.rowsFromTransactionCacheAndDatabase(op.Table, op.Where)
	if err != nil {
		return ovsdb.ResultFromError(err), nil
	}

	update := updates.ModelUpdates{}
	for uuid, old := range rows {
		err := update.AddOperation(t.Model, op.Table, uuid, old, op)
		if err != nil {
			return ovsdb.ResultFromError(err), nil
		}
	}

	// FIXME: We need to filter the returned columns
	return ovsdb.OperationResult{Count: len(rows)}, &update
}

func (t *Transaction) Mutate(op *ovsdb.Operation) (ovsdb.OperationResult, *updates.ModelUpdates) {
	rows, err := t.rowsFromTransactionCacheAndDatabase(op.Table, op.Where)
	if err != nil {
		return ovsdb.ResultFromError(err), nil
	}

	update := updates.ModelUpdates{}
	for uuid, old := range rows {
		err := update.AddOperation(t.Model, op.Table, uuid, old, op)
		if err != nil {
			return ovsdb.ResultFromError(err), nil
		}
	}

	return ovsdb.OperationResult{Count: len(rows)}, &update
}

func (t *Transaction) Delete(op *ovsdb.Operation) (ovsdb.OperationResult, *updates.ModelUpdates) {
	rows, err := t.rowsFromTransactionCacheAndDatabase(op.Table, op.Where)
	if err != nil {
		return ovsdb.ResultFromError(err), nil
	}

	update := updates.ModelUpdates{}
	for uuid, row := range rows {
		err := update.AddOperation(t.Model, op.Table, uuid, row, op)
		if err != nil {
			return ovsdb.ResultFromError(err), nil
		}

		// track delete operation in transaction to complement cache
		t.DeletedRows[uuid] = struct{}{}
	}

	return ovsdb.OperationResult{Count: len(rows)}, &update
}

func (t *Transaction) Wait(table string, timeout *int, where []ovsdb.Condition, columns []string, until string, rows []ovsdb.Row) ovsdb.OperationResult {
	start := time.Now()

	if until != "!=" && until != "==" {
		return ovsdb.ResultFromError(&ovsdb.NotSupported{})
	}

	dbModel := t.Model
	realTable := dbModel.Schema.Table(table)
	if realTable == nil {
		return ovsdb.ResultFromError(&ovsdb.NotSupported{})
	}
	model, err := dbModel.NewModel(table)
	if err != nil {
		return ovsdb.ResultFromError(err)
	}

Loop:
	for {
		var filteredRows []ovsdb.Row
		foundRowModels, err := t.rowsFromTransactionCacheAndDatabase(table, where)
		if err != nil {
			return ovsdb.ResultFromError(err)
		}

		m := dbModel.Mapper
		for _, rowModel := range foundRowModels {
			info, err := dbModel.NewModelInfo(rowModel)
			if err != nil {
				return ovsdb.ResultFromError(err)
			}

			foundMatch := true
			for _, column := range columns {
				columnSchema := info.Metadata.TableSchema.Column(column)
				for _, r := range rows {
					i, err := dbModel.NewModelInfo(model)
					if err != nil {
						return ovsdb.ResultFromError(err)
					}
					err = dbModel.Mapper.GetRowData(&r, i)
					if err != nil {
						return ovsdb.ResultFromError(err)
					}
					x, err := i.FieldByColumn(column)
					if err != nil {
						return ovsdb.ResultFromError(err)
					}

					// check to see if field value is default for given rows
					// if it is default (not provided) we shouldn't try to compare
					// for equality
					if ovsdb.IsDefaultValue(columnSchema, x) {
						continue
					}
					y, err := info.FieldByColumn(column)
					if err != nil {
						return ovsdb.ResultFromError(err)
					}
					if !reflect.DeepEqual(x, y) {
						foundMatch = false
					}
				}
			}

			if foundMatch {
				resultRow, err := m.NewRow(info)
				if err != nil {
					return ovsdb.ResultFromError(err)
				}
				filteredRows = append(filteredRows, resultRow)
			}

		}

		if until == "==" && len(filteredRows) == len(rows) {
			return ovsdb.OperationResult{}
		} else if until == "!=" && len(filteredRows) != len(rows) {
			return ovsdb.OperationResult{}
		}

		if timeout != nil {
			// TODO(trozet): this really shouldn't just break and loop on a time interval
			// Really this client handler should pause, wait for another handler to update the DB
			// and then try again. However the server is single threaded for now and not capable of
			// doing something like that.
			if time.Since(start) > time.Duration(*timeout)*time.Millisecond {
				break Loop
			}
		}
		time.Sleep(200 * time.Millisecond)
	}

	return ovsdb.ResultFromError(&ovsdb.TimedOut{})
}

func (t *Transaction) Commit(table string, durable bool) ovsdb.OperationResult {
	return ovsdb.ResultFromError(&ovsdb.NotSupported{})
}

func (t *Transaction) Abort(table string) ovsdb.OperationResult {
	return ovsdb.ResultFromError(&ovsdb.NotSupported{})
}

func (t *Transaction) Comment(table string, comment string) ovsdb.OperationResult {
	return ovsdb.ResultFromError(&ovsdb.NotSupported{})
}

func (t *Transaction) Assert(table, lock string) ovsdb.OperationResult {
	return ovsdb.ResultFromError(&ovsdb.NotSupported{})
}
