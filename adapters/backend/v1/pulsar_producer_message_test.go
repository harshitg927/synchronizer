package backend

import (
	"testing"

	"github.com/kubescape/synchronizer/messaging"
	"github.com/stretchr/testify/assert"
)

// TestNewProducerMessage_SelfLoopTagging guards the producer/consumer contract:
// only the synchronizer server's own messages (its producer key) may carry the
// self-produced source header that the read loop uses to skip messages
// (messaging.IsSelfProducedMessage). External producers such as
// event-ingester-service supply their own key and must NOT be tagged, otherwise
// the server silently drops their backend->cluster messages.
func TestNewProducerMessage_SelfLoopTagging(t *testing.T) {
	payload := []byte(`{"test":"test"}`)

	t.Run("synchronizer server message is tagged self-produced", func(t *testing.T) {
		msg := NewProducerMessage(messaging.SynchronizerServerProducerKey, "acc", "cluster", messaging.MsgPropEventValuePutObjectMessage, payload)
		assert.Equal(t, messaging.SynchronizerServerProducerKey, msg.Key)
		assert.Equal(t, messaging.MsgPropProducerSourceSynchronizerServer, msg.Properties[messaging.MsgPropProducerSource])
		assert.True(t, messaging.IsSelfProducedMessage(msg.Properties, msg.Key))
	})

	t.Run("external producer message is not tagged self-produced", func(t *testing.T) {
		const externalKey = "eventIngesterProducer"
		msg := NewProducerMessage(externalKey, "acc", "cluster", messaging.MsgPropEventValuePutObjectMessage, payload)
		assert.Equal(t, externalKey, msg.Key)
		assert.NotContains(t, msg.Properties, messaging.MsgPropProducerSource)
		assert.False(t, messaging.IsSelfProducedMessage(msg.Properties, msg.Key))
	})
}
