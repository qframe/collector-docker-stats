package qcollector_docker_stats

import (
	"testing"
	"github.com/stretchr/testify/assert"
)


func TestSplitLabels(t *testing.T) {
	exp := map[string]string{
		"key1": "val1",
		"key2": "val2",
	}
	got := SplitLabels([]string{"key1=val1","key2=val2"})
	assert.Equal(t, exp, got)
}