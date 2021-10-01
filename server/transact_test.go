package server

import (
	"testing"

	"github.com/google/uuid"
	"github.com/ovn-org/libovsdb/mapper"
	"github.com/ovn-org/libovsdb/model"
	"github.com/ovn-org/libovsdb/ovsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMutateOp(t *testing.T) {
	defDB, err := model.NewDBModel("Open_vSwitch", map[string]model.Model{
		"Open_vSwitch": &ovsType{},
		"Bridge":       &bridgeType{}})
	if err != nil {
		t.Fatal(err)
	}
	schema, err := getSchema()
	if err != nil {
		t.Fatal(err)
	}
	ovsDB := NewInMemoryDatabase(map[string]*model.DBModel{"Open_vSwitch": defDB})
	o, err := NewOvsdbServer(ovsDB, DatabaseModel{
		Model: defDB, Schema: schema})
	require.Nil(t, err)

	ovsUUID := uuid.NewString()
	bridgeUUID := uuid.NewString()

	m := mapper.NewMapper(schema)

	ovs := ovsType{}
	ovsRow, err := m.NewRow("Open_vSwitch", &ovs)
	require.Nil(t, err)

	bridge := bridgeType{
		Name: "foo",
		ExternalIds: map[string]string{
			"foo":   "bar",
			"baz":   "qux",
			"waldo": "fred",
		},
	}
	bridgeRow, err := m.NewRow("Bridge", &bridge)
	require.Nil(t, err)

	res, updates := o.Insert("Open_vSwitch", "Open_vSwitch", ovsUUID, ovsRow)
	_, err = ovsdb.CheckOperationResults([]ovsdb.OperationResult{res}, []ovsdb.Operation{{Op: "insert"}})
	require.Nil(t, err)

	res, update2 := o.Insert("Open_vSwitch", "Bridge", bridgeUUID, bridgeRow)
	_, err = ovsdb.CheckOperationResults([]ovsdb.OperationResult{res}, []ovsdb.Operation{{Op: "insert"}})
	require.Nil(t, err)

	updates.Merge(update2)
	err = o.db.Commit("Open_vSwitch", updates)
	require.NoError(t, err)

	gotResult, gotUpdate := o.Mutate(
		"Open_vSwitch",
		"Open_vSwitch",
		[]ovsdb.Condition{
			ovsdb.NewCondition("_uuid", ovsdb.ConditionEqual, ovsdb.UUID{GoUUID: ovsUUID}),
		},
		[]ovsdb.Mutation{
			*ovsdb.NewMutation("bridges", ovsdb.MutateOperationInsert, ovsdb.UUID{GoUUID: bridgeUUID}),
		},
	)
	assert.Equal(t, ovsdb.OperationResult{Count: 1}, gotResult)

	bridgeSet, err := ovsdb.NewOvsSet([]ovsdb.UUID{{GoUUID: bridgeUUID}})
	assert.Nil(t, err)
	assert.Equal(t, ovsdb.TableUpdates{
		"Open_vSwitch": ovsdb.TableUpdate{
			ovsUUID: &ovsdb.RowUpdate{
				Old: &ovsdb.Row{
					"_uuid": ovsdb.UUID{GoUUID: ovsUUID},
				},
				New: &ovsdb.Row{
					"_uuid":   ovsdb.UUID{GoUUID: ovsUUID},
					"bridges": bridgeSet,
				},
			},
		},
	}, gotUpdate)

	keyDelete, err := ovsdb.NewOvsSet([]string{"foo"})
	assert.Nil(t, err)
	keyValueDelete, err := ovsdb.NewOvsMap(map[string]string{"baz": "qux"})
	assert.Nil(t, err)
	gotResult, gotUpdate = o.Mutate(
		"Open_vSwitch",
		"Bridge",
		[]ovsdb.Condition{
			ovsdb.NewCondition("_uuid", ovsdb.ConditionEqual, ovsdb.UUID{GoUUID: bridgeUUID}),
		},
		[]ovsdb.Mutation{
			*ovsdb.NewMutation("external_ids", ovsdb.MutateOperationDelete, keyDelete),
			*ovsdb.NewMutation("external_ids", ovsdb.MutateOperationDelete, keyValueDelete),
		},
	)
	assert.Equal(t, ovsdb.OperationResult{Count: 1}, gotResult)

	oldExternalIds, err := ovsdb.NewOvsMap(bridge.ExternalIds)
	assert.Nil(t, err)
	newExternalIds, err := ovsdb.NewOvsMap(map[string]string{"waldo": "fred"})
	assert.Nil(t, err)
	assert.Equal(t, ovsdb.TableUpdates{
		"Bridge": ovsdb.TableUpdate{
			bridgeUUID: &ovsdb.RowUpdate{
				Old: &ovsdb.Row{
					"_uuid":        ovsdb.UUID{GoUUID: bridgeUUID},
					"name":         "foo",
					"external_ids": oldExternalIds,
				},
				New: &ovsdb.Row{
					"_uuid":        ovsdb.UUID{GoUUID: bridgeUUID},
					"name":         "foo",
					"external_ids": newExternalIds,
				},
			},
		},
	}, gotUpdate)
}

func TestMultipleOps(t *testing.T) {
	defDB, err := model.NewDBModel("Open_vSwitch", map[string]model.Model{
		"Bridge": &bridgeType{}})
	if err != nil {
		t.Fatal(err)
	}
	schema, err := getSchema()
	if err != nil {
		t.Fatal(err)
	}
	ovsDB := NewInMemoryDatabase(map[string]*model.DBModel{"Open_vSwitch": defDB})
	o, err := NewOvsdbServer(ovsDB, DatabaseModel{
		Model: defDB, Schema: schema})
	require.Nil(t, err)

	bridgeUUID := uuid.NewString()
	m := mapper.NewMapper(schema)

	bridge := bridgeType{
		Name: "a_bridge_to_nowhere",
		Ports: []string{
			"port1",
			"port10",
		},
	}
	bridgeRow, err := m.NewRow("Bridge", &bridge)
	require.Nil(t, err)

	res, updates := o.Insert("Open_vSwitch", "Bridge", bridgeUUID, bridgeRow)
	_, err = ovsdb.CheckOperationResults([]ovsdb.OperationResult{res}, []ovsdb.Operation{{Op: "insert"}})
	require.Nil(t, err)

	err = o.db.Commit("Open_vSwitch", updates)
	require.NoError(t, err)

	var ops []ovsdb.Operation
	var op ovsdb.Operation

	op = ovsdb.Operation{
		Table: "Bridge",
		Where: []ovsdb.Condition{
			ovsdb.NewCondition("_uuid", ovsdb.ConditionEqual, ovsdb.UUID{GoUUID: bridgeUUID}),
		},
		Op: ovsdb.OperationMutate,
		Mutations: []ovsdb.Mutation{
			// *ovsdb.NewMutation("ports", ovsdb.MutateOperationInsert, ovsdb.UUID{GoUUID: "portA"}),
			// *ovsdb.NewMutation("ports", ovsdb.MutateOperationDelete, []ovsdb.UUID{{GoUUID: "port10"}}),
			*ovsdb.NewMutation("ports", ovsdb.MutateOperationDelete, ovsdb.UUID{GoUUID: "port10"}),
		},
	}
	ops = append(ops, op)

	op = ovsdb.Operation{
		Table: "Bridge",
		Where: []ovsdb.Condition{
			ovsdb.NewCondition("_uuid", ovsdb.ConditionEqual, ovsdb.UUID{GoUUID: bridgeUUID}),
		},
		Op: ovsdb.OperationMutate,
		Mutations: []ovsdb.Mutation{
			*ovsdb.NewMutation("ports", ovsdb.MutateOperationInsert, ovsdb.UUID{GoUUID: "portA"}),
			// *ovsdb.NewMutation("ports", ovsdb.MutateOperationDelete, ovsdb.UUID{GoUUID: "port10"}),
		},
	}
	ops = append(ops, op)

	op = ovsdb.Operation{
		Table: "Bridge",
		Where: []ovsdb.Condition{
			ovsdb.NewCondition("_uuid", ovsdb.ConditionEqual, ovsdb.UUID{GoUUID: bridgeUUID}),
		},
		Op: ovsdb.OperationMutate,
		Mutations: []ovsdb.Mutation{
			*ovsdb.NewMutation("ports", ovsdb.MutateOperationInsert, ovsdb.UUID{GoUUID: "portB"}),
		},
	}
	ops = append(ops, op)

	op = ovsdb.Operation{
		Table: "Bridge",
		Where: []ovsdb.Condition{
			ovsdb.NewCondition("_uuid", ovsdb.ConditionEqual, ovsdb.UUID{GoUUID: bridgeUUID}),
		},
		Op: ovsdb.OperationMutate,
		Mutations: []ovsdb.Mutation{
			*ovsdb.NewMutation("ports", ovsdb.MutateOperationInsert, ovsdb.UUID{GoUUID: "portB"}),
			*ovsdb.NewMutation("ports", ovsdb.MutateOperationInsert, ovsdb.UUID{GoUUID: "portC"}),
		},
	}
	ops = append(ops, op)

	results, updates := o.transact("Open_vSwitch", ops)
	require.Len(t, results, len(ops))
	for _, result := range results {
		assert.Equal(t, "", result.Error)
	}

	err = o.db.Commit("Open_vSwitch", updates)
	require.NoError(t, err)

	oldPorts, err := ovsdb.NewOvsSet([]ovsdb.UUID{{GoUUID: "port1"}, {GoUUID: "port10"}})
	assert.Nil(t, err)
	newPorts, err := ovsdb.NewOvsSet([]ovsdb.UUID{{GoUUID: "port1"}, {GoUUID: "portA"}, {GoUUID: "portB"}, {GoUUID: "portC"}})
	assert.Nil(t, err)

	assert.Equal(t, ovsdb.TableUpdates{
		"Bridge": ovsdb.TableUpdate{
			bridgeUUID: &ovsdb.RowUpdate{
				Old: &ovsdb.Row{
					"_uuid": ovsdb.UUID{GoUUID: bridgeUUID},
					"name":  "a_bridge_to_nowhere",
					"ports": oldPorts,
				},
				New: &ovsdb.Row{
					"_uuid": ovsdb.UUID{GoUUID: bridgeUUID},
					"name":  "a_bridge_to_nowhere",
					"ports": newPorts,
				},
			},
		},
	}, updates)

}
