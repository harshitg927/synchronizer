package backend

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/kubescape/synchronizer/domain"
	"github.com/kubescape/synchronizer/messaging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockMessageProducer struct {
	messages []mockProducedMessage
}

type mockProducedMessage struct {
	eventType string
	payload   []byte
}

func (m *mockMessageProducer) ProduceMessage(_ context.Context, _ domain.ClientIdentifier, eventType string, payload []byte, _ ...map[string]string) error {
	m.messages = append(m.messages, mockProducedMessage{eventType: eventType, payload: payload})
	return nil
}

func (m *mockMessageProducer) ProduceMessageWithoutIdentifier(_ context.Context, eventType string, payload []byte) error {
	m.messages = append(m.messages, mockProducedMessage{eventType: eventType, payload: payload})
	return nil
}

func TestSendSingleConnectedClientsMessage_IncludesClusterUID(t *testing.T) {
	producer := &mockMessageProducer{}
	client := NewClient(producer, nil, nil)

	id := domain.ClientIdentifier{
		Account:        "test-account",
		Cluster:        "test-cluster",
		ConnectionId:   "conn-123",
		ConnectionTime: time.Now(),
		HelmVersion:    "v1.0.0",
		SyncVersion:    "v0.0.1",
		GitVersion:     "v1.28.0",
		CloudProvider:  "azure",
		ClusterUID:     "abc-123-uid",
	}

	ctx := context.WithValue(context.Background(), domain.ContextKeyClientIdentifier, id)

	err := client.Start(ctx)
	require.NoError(t, err)

	// Should have sent ServerConnected + ConnectedClients
	require.Len(t, producer.messages, 2)

	// Verify ConnectedClients message
	connectedMsg := producer.messages[1]
	assert.Equal(t, messaging.MsgPropEventValueConnectedClientsMessage, connectedMsg.eventType)

	var msg messaging.ConnectedClientsMessage
	err = json.Unmarshal(connectedMsg.payload, &msg)
	require.NoError(t, err)
	require.Len(t, msg.Clients, 1)

	assert.Equal(t, "test-account", msg.Clients[0].Account)
	assert.Equal(t, "test-cluster", msg.Clients[0].Cluster)
	assert.Equal(t, "abc-123-uid", msg.Clients[0].ClusterUID)
	assert.Equal(t, "azure", msg.Clients[0].CloudProvider)
	assert.Equal(t, "v1.28.0", msg.Clients[0].GitVersion)
}

func TestSendSingleConnectedClientsMessage_EmptyClusterUID(t *testing.T) {
	producer := &mockMessageProducer{}
	client := NewClient(producer, nil, nil)

	id := domain.ClientIdentifier{
		Account:    "test-account",
		Cluster:    "test-cluster",
		ClusterUID: "",
	}

	ctx := context.WithValue(context.Background(), domain.ContextKeyClientIdentifier, id)

	err := client.Start(ctx)
	require.NoError(t, err)
	require.Len(t, producer.messages, 2)

	var msg messaging.ConnectedClientsMessage
	err = json.Unmarshal(producer.messages[1].payload, &msg)
	require.NoError(t, err)
	require.Len(t, msg.Clients, 1)

	assert.Equal(t, "", msg.Clients[0].ClusterUID)
}
