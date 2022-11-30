package server

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/ovn-org/libovsdb/database"
	"github.com/ovn-org/libovsdb/model"
	"github.com/ovn-org/libovsdb/ovsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	. "github.com/ovn-org/libovsdb/test"
)

func TestExpandNamedUUID(t *testing.T) {
	testUUID := uuid.NewString()
	testUUID1 := uuid.NewString()
	tests := []struct {
		name       string
		namedUUIDs map[string]ovsdb.UUID
		value      interface{}
		expected   interface{}
	}{
		{
			"uuid",
			map[string]ovsdb.UUID{"foo": {GoUUID: testUUID}},
			ovsdb.UUID{GoUUID: "foo"},
			ovsdb.UUID{GoUUID: testUUID},
		},
		{
			"set",
			map[string]ovsdb.UUID{"foo": {GoUUID: testUUID}},
			ovsdb.OvsSet{GoSet: []interface{}{ovsdb.UUID{GoUUID: "foo"}}},
			ovsdb.OvsSet{GoSet: []interface{}{ovsdb.UUID{GoUUID: testUUID}}},
		},
		{
			"set multiple",
			map[string]ovsdb.UUID{"foo": {GoUUID: testUUID}, "bar": {GoUUID: testUUID1}},
			ovsdb.OvsSet{GoSet: []interface{}{ovsdb.UUID{GoUUID: "foo"}, ovsdb.UUID{GoUUID: "bar"}, ovsdb.UUID{GoUUID: "baz"}}},
			ovsdb.OvsSet{GoSet: []interface{}{ovsdb.UUID{GoUUID: testUUID}, ovsdb.UUID{GoUUID: testUUID1}, ovsdb.UUID{GoUUID: "baz"}}},
		},
		{
			"map key",
			map[string]ovsdb.UUID{"foo": {GoUUID: testUUID}},
			ovsdb.OvsMap{GoMap: map[interface{}]interface{}{ovsdb.UUID{GoUUID: "foo"}: "foo"}},
			ovsdb.OvsMap{GoMap: map[interface{}]interface{}{ovsdb.UUID{GoUUID: testUUID}: "foo"}},
		},
		{
			"map values",
			map[string]ovsdb.UUID{"foo": {GoUUID: testUUID}},
			ovsdb.OvsMap{GoMap: map[interface{}]interface{}{"foo": ovsdb.UUID{GoUUID: "foo"}}},
			ovsdb.OvsMap{GoMap: map[interface{}]interface{}{"foo": ovsdb.UUID{GoUUID: testUUID}}},
		},
		{
			"map key and values",
			map[string]ovsdb.UUID{"foo": {GoUUID: testUUID}, "bar": {GoUUID: testUUID1}},
			ovsdb.OvsMap{GoMap: map[interface{}]interface{}{ovsdb.UUID{GoUUID: "foo"}: ovsdb.UUID{GoUUID: "bar"}}},
			ovsdb.OvsMap{GoMap: map[interface{}]interface{}{ovsdb.UUID{GoUUID: testUUID}: ovsdb.UUID{GoUUID: testUUID1}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := expandNamedUUID(tt.value, tt.namedUUIDs)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestOvsdbServerMonitor(t *testing.T) {
	dbModel, err := GetModel()
	require.NoError(t, err)
	ovsDB := database.NewInMemoryDatabase(map[string]model.ClientDBModel{"Open_vSwitch": dbModel.Client()})
	schema := dbModel.Schema

	o, err := NewOvsdbServer(ovsDB, dbModel)
	require.Nil(t, err)
	requests := make(map[string]ovsdb.MonitorRequest)
	for table, tableSchema := range schema.Tables {
		var columns []string
		for column := range tableSchema.Columns {
			columns = append(columns, column)
		}
		requests[table] = ovsdb.MonitorRequest{
			Columns: columns,
			Select:  ovsdb.NewDefaultMonitorSelect(),
		}
	}

	fooUUID := uuid.NewString()
	barUUID := uuid.NewString()
	bazUUID := uuid.NewString()
	quuxUUID := uuid.NewString()

	operations := []ovsdb.Operation{
		{
			Op:       ovsdb.OperationInsert,
			Table:    "Bridge",
			UUIDName: fooUUID,
			Row:      ovsdb.Row{"name": "foo"},
		},
		{
			Op:       ovsdb.OperationInsert,
			Table:    "Bridge",
			UUIDName: barUUID,
			Row:      ovsdb.Row{"name": "bar"},
		},
		{
			Op:       ovsdb.OperationInsert,
			Table:    "Bridge",
			UUIDName: bazUUID,
			Row:      ovsdb.Row{"name": "baz"},
		},
		{
			Op:       ovsdb.OperationInsert,
			Table:    "Bridge",
			UUIDName: quuxUUID,
			Row:      ovsdb.Row{"name": "quux"},
		},
	}
	transaction := database.NewTransaction(dbModel, "Open_vSwitch", o.db, &o.logger)
	_, updates := transaction.Transact(operations)
	err = o.db.Commit("Open_vSwitch", uuid.New(), updates)
	require.NoError(t, err)

	db, err := json.Marshal("Open_vSwitch")
	require.Nil(t, err)
	value, err := json.Marshal("foo")
	require.Nil(t, err)
	rJSON, err := json.Marshal(requests)
	require.Nil(t, err)
	args := []json.RawMessage{db, value, rJSON}
	reply := &ovsdb.TableUpdates{}
	err = o.Monitor(nil, args, reply)
	require.Nil(t, err)
	expected := &ovsdb.TableUpdates{
		"Bridge": {
			fooUUID: &ovsdb.RowUpdate{
				New: &ovsdb.Row{
					"_uuid": ovsdb.UUID{GoUUID: fooUUID},
					"name":  "foo",
				},
			},
			barUUID: &ovsdb.RowUpdate{
				New: &ovsdb.Row{
					"_uuid": ovsdb.UUID{GoUUID: barUUID},
					"name":  "bar",
				},
			},
			bazUUID: &ovsdb.RowUpdate{
				New: &ovsdb.Row{
					"_uuid": ovsdb.UUID{GoUUID: bazUUID},
					"name":  "baz",
				},
			},
			quuxUUID: &ovsdb.RowUpdate{
				New: &ovsdb.Row{
					"_uuid": ovsdb.UUID{GoUUID: quuxUUID},
					"name":  "quux",
				},
			},
		},
	}
	assert.Equal(t, expected, reply)
}
