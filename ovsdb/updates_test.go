package ovsdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRowUpdateInsert(t *testing.T) {
	u1 := RowUpdate{Old: nil, New: &Row{}}
	u2 := RowUpdate{Old: &Row{}, New: &Row{}}
	u3 := RowUpdate{Old: &Row{}, New: nil}

	assert.True(t, u1.Insert())
	assert.False(t, u2.Insert())
	assert.False(t, u3.Insert())
}

func TestRowUpdateModify(t *testing.T) {
	u1 := RowUpdate{Old: nil, New: &Row{}}
	u2 := RowUpdate{Old: &Row{}, New: &Row{}}
	u3 := RowUpdate{Old: &Row{}, New: nil}

	assert.False(t, u1.Modify())
	assert.True(t, u2.Modify())
	assert.False(t, u3.Modify())
}

func TestRowUpdateDelete(t *testing.T) {
	u1 := RowUpdate{Old: nil, New: &Row{}}
	u2 := RowUpdate{Old: &Row{}, New: &Row{}}
	u3 := RowUpdate{Old: &Row{}, New: nil}

	assert.False(t, u1.Delete())
	assert.False(t, u2.Delete())
	assert.True(t, u3.Delete())
}
