package messaging

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/kubescape/synchronizer/adapters"
	"github.com/kubescape/synchronizer/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type unrelatedMockAdapter struct {
	*adapters.MockAdapter
}

func (u *unrelatedMockAdapter) IsRelated(_ context.Context, _ domain.ClientIdentifier) bool {
	return false
}

func newHandlerTestAdapter(callbacks domain.Callbacks) *adapters.MockAdapter {
	adapter := adapters.NewMockAdapter(false)
	adapter.RegisterCallbacks(context.Background(), callbacks)
	return adapter
}

func TestMessageHandler_SkipsUnrelatedClient(t *testing.T) {
	putCalled := false
	base := newHandlerTestAdapter(domain.Callbacks{
		PutObject: func(_ context.Context, _ domain.KindName, _ string, _ []byte) error {
			putCalled = true
			return nil
		},
	})

	payload, err := json.Marshal(PutObjectMessage{
		Kind:  "/v1/configmaps",
		Name:  "test",
		Depth: 1,
	})
	require.NoError(t, err)

	handler := &MessageHandler{Name: "test-reader"}
	err = handler.Handle(context.Background(), &unrelatedMockAdapter{MockAdapter: base}, IncomingMessage{
		ID: "msg-1",
		Properties: map[string]string{
			MsgPropAccount: "account",
			MsgPropCluster: "cluster",
			MsgPropEvent:   MsgPropEventValuePutObjectMessage,
		},
		Payload: payload,
	})
	require.NoError(t, err)
	assert.False(t, putCalled)
}

func TestMessageHandler_DispatchesMessages(t *testing.T) {
	const objectName = "test"

	tests := []struct {
		name      string
		eventType string
		payload   any
		callbacks domain.Callbacks
	}{
		{
			name:      "PutObject",
			eventType: MsgPropEventValuePutObjectMessage,
			payload: PutObjectMessage{
				Kind:  "/v1/configmaps",
				Name:  objectName,
				Depth: 1,
			},
			callbacks: domain.Callbacks{
				PutObject: func(_ context.Context, id domain.KindName, _ string, _ []byte) error {
					assert.Equal(t, objectName, id.Name)
					return nil
				},
			},
		},
		{
			name:      "GetObject",
			eventType: MsgPropEventValueGetObjectMessage,
			payload: GetObjectMessage{
				Kind:  "/v1/configmaps",
				Name:  objectName,
				Depth: 1,
			},
			callbacks: domain.Callbacks{
				GetObject: func(_ context.Context, id domain.KindName, _ []byte) error {
					assert.Equal(t, objectName, id.Name)
					return nil
				},
			},
		},
		{
			name:      "PatchObject",
			eventType: MsgPropEventValuePatchObjectMessage,
			payload: PatchObjectMessage{
				Kind:  "/v1/configmaps",
				Name:  objectName,
				Depth: 1,
			},
			callbacks: domain.Callbacks{
				PatchObject: func(_ context.Context, id domain.KindName, _ string, _ []byte) error {
					assert.Equal(t, objectName, id.Name)
					return nil
				},
			},
		},
		{
			name:      "VerifyObject",
			eventType: MsgPropEventValueVerifyObjectMessage,
			payload: VerifyObjectMessage{
				Kind:  "/v1/configmaps",
				Name:  objectName,
				Depth: 1,
			},
			callbacks: domain.Callbacks{
				VerifyObject: func(_ context.Context, id domain.KindName, _ string) error {
					assert.Equal(t, objectName, id.Name)
					return nil
				},
			},
		},
		{
			name:      "DeleteObject",
			eventType: MsgPropEventValueDeleteObjectMessage,
			payload: DeleteObjectMessage{
				Kind:  "/v1/configmaps",
				Name:  objectName,
				Depth: 1,
			},
			callbacks: domain.Callbacks{
				DeleteObject: func(_ context.Context, id domain.KindName) error {
					assert.Equal(t, objectName, id.Name)
					return nil
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adapter := newHandlerTestAdapter(tt.callbacks)
			payload, err := json.Marshal(tt.payload)
			require.NoError(t, err)

			handler := &MessageHandler{Name: "test-reader"}
			err = handler.Handle(context.Background(), adapter, IncomingMessage{
				ID: "msg-1",
				Properties: map[string]string{
					MsgPropAccount: "account",
					MsgPropCluster: "cluster",
					MsgPropEvent:   tt.eventType,
				},
				Payload: payload,
			})
			require.NoError(t, err)
		})
	}
}

func TestMessageHandler_UnknownMessageType(t *testing.T) {
	adapter := newHandlerTestAdapter(domain.Callbacks{})
	handler := &MessageHandler{Name: "test-reader"}

	err := handler.Handle(context.Background(), adapter, IncomingMessage{
		ID: "msg-1",
		Properties: map[string]string{
			MsgPropAccount: "account",
			MsgPropCluster: "cluster",
			MsgPropEvent:   "UnknownEvent",
		},
		Payload: []byte(`{}`),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown message type")
}

func TestMessageHandler_ReconciliationMutexSerializes(t *testing.T) {
	var mu sync.Mutex
	concurrent := 0
	maxConcurrent := 0

	adapter := newHandlerTestAdapter(domain.Callbacks{
		Batch: func(_ context.Context, _ domain.Kind, _ domain.BatchType, _ domain.BatchItems) error {
			mu.Lock()
			concurrent++
			if concurrent > maxConcurrent {
				maxConcurrent = concurrent
			}
			mu.Unlock()

			time.Sleep(20 * time.Millisecond)

			mu.Lock()
			concurrent--
			mu.Unlock()
			return nil
		},
	})

	payload, err := json.Marshal(ReconciliationRequestMessage{
		Depth: 1,
		KindToObjects: map[string][]ReconciliationRequestObject{
			"apps/v1/deployments": {{Name: "test"}},
		},
	})
	require.NoError(t, err)

	handler := &MessageHandler{Name: "test-reader"}
	msg := IncomingMessage{
		ID: "msg-1",
		Properties: map[string]string{
			MsgPropAccount: "account",
			MsgPropCluster: "cluster",
			MsgPropEvent:   MsgPropEventValueReconciliationRequestMessage,
		},
		Payload: payload,
	}

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.NoError(t, handler.Handle(context.Background(), adapter, msg))
		}()
	}
	wg.Wait()

	assert.Equal(t, 1, maxConcurrent)
}
