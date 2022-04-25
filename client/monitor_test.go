package client

import (
	"encoding/json"
	"testing"

	"github.com/ovn-org/libovsdb/cache"
	"github.com/ovn-org/libovsdb/model"
	"github.com/ovn-org/libovsdb/ovsdb"
	"github.com/stretchr/testify/assert"
)

func TestWithTable(t *testing.T) {
	client, err := newOVSDBClient(defDB)
	assert.NoError(t, err)
	m := newMonitor()
	opt := WithTable(&OpenvSwitch{})

	err = opt(client, m)
	assert.NoError(t, err)

	assert.Equal(t, 1, len(m.Tables))
}

func TestWithTableAndFields(t *testing.T) {
	client, err := newOVSDBClient(defDB)
	assert.NoError(t, err)

	// FIXME(FF): this is a mess and needs to be cleaned up!  BEGIN
	var s ovsdb.DatabaseSchema
	err = json.Unmarshal([]byte(schema), &s)
	assert.NoError(t, err)
	clientDBModel, err := model.NewClientDBModel("Open_vSwitch", map[string]model.Model{
		"Bridge":       &Bridge{},
		"Open_vSwitch": &OpenvSwitch{},
	})
	assert.NoError(t, err)
	dbModel, errs := model.NewDatabaseModel(s, clientDBModel)
	assert.Empty(t, errs)
	client.primaryDB().cache, err = cache.NewTableCache(dbModel, nil, nil)
	assert.NoError(t, err)
	// FIXME(FF): this is a mess and needs to be cleaned up!  END

	m := newMonitor()
	ovs := OpenvSwitch{}

	// FIXME(FF): uncomment this line when the mess is fixed
	// opt := WithTable(&ovs, &ovs.Bridges, &ovs.CurCfg)
	opt := WithTable(&ovs)

	err = opt(client, m)
	assert.NoError(t, err)

	assert.Equal(t, 1, len(m.Tables))
	// FIXME(FF): uncomment this line when the mess is fixed
	// assert.ElementsMatch(t, []string{"bridges", "curcfg"}, m.Tables[0].Fields)
}
