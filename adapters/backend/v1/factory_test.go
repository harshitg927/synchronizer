package backend

import (
	"testing"

	"github.com/kubescape/synchronizer/config"
	"github.com/kubescape/synchronizer/messaging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewFromConfig_NoPulsarConfig(t *testing.T) {
	components, err := newFromConfig(config.Config{})
	require.NoError(t, err)
	assert.Nil(t, components)
}

func TestNewFromConfig_RegistersWithMessaging(t *testing.T) {
	components, err := messaging.NewFromConfig(config.Config{})
	require.NoError(t, err)
	assert.Nil(t, components)
}
