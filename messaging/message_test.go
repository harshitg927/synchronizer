package messaging

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsSelfProducedMessage(t *testing.T) {
	tests := []struct {
		name       string
		properties map[string]string
		messageKey string
		want       bool
	}{
		{
			name:       "header only",
			properties: map[string]string{MsgPropProducerSource: MsgPropProducerSourceSynchronizerServer},
			messageKey: "",
			want:       true,
		},
		{
			name:       "legacy key only",
			properties: map[string]string{},
			messageKey: SynchronizerServerProducerKey,
			want:       true,
		},
		{
			name: "header and key",
			properties: map[string]string{
				MsgPropProducerSource: MsgPropProducerSourceSynchronizerServer,
			},
			messageKey: SynchronizerServerProducerKey,
			want:       true,
		},
		{
			name:       "neither",
			properties: map[string]string{MsgPropEvent: MsgPropEventValuePutObjectMessage},
			messageKey: "other-key",
			want:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IsSelfProducedMessage(tt.properties, tt.messageKey))
		})
	}
}

func TestBuildProducerProperties(t *testing.T) {
	props := BuildProducerProperties("account1", "cluster1", MsgPropEventValuePutObjectMessage,
		map[string]string{MsgPropResourceKindResource: "configmaps"})

	assert.Equal(t, "account1", props[MsgPropAccount])
	assert.Equal(t, "cluster1", props[MsgPropCluster])
	assert.Equal(t, MsgPropEventValuePutObjectMessage, props[MsgPropEvent])
	assert.Equal(t, MsgPropProducerSourceSynchronizerServer, props[MsgPropProducerSource])
	assert.Equal(t, "configmaps", props[MsgPropResourceKindResource])
	assert.NotEmpty(t, props[MsgPropTimestamp])
}
