package messaging

import (
	"testing"

	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecordProducerMessage(t *testing.T) {
	properties := map[string]string{
		MsgPropEvent:                MsgPropEventValuePutObjectMessage,
		MsgPropAccount:              "account1",
		MsgPropCluster:              "cluster1",
		MsgPropResourceKindResource: "configmaps",
	}

	RecordProducerMessage("pulsar", prometheusStatusSuccess, properties, 128)
	RecordProducerMessage("pulsar", prometheusStatusError, nil, 0)

	mqMetric, err := mqProducerMessagesProducedCounter.GetMetricWith(producerMetricLabels(
		"pulsar", prometheusStatusSuccess, properties,
	))
	require.NoError(t, err)

	mqMetricDTO := &dto.Metric{}
	require.NoError(t, mqMetric.Write(mqMetricDTO))
	assert.Equal(t, float64(1), mqMetricDTO.GetCounter().GetValue())

	pulsarMetric, err := pulsarProducerMessagesProducedCounter.GetMetricWith(map[string]string{
		prometheusStatusLabel:    prometheusStatusSuccess,
		prometheusEventTypeLabel: MsgPropEventValuePutObjectMessage,
		prometheusKindLabel:      "configmaps",
		prometheusAccountLabel:   "account1",
		prometheusClusterLabel:   "cluster1",
	})
	require.NoError(t, err)

	pulsarMetricDTO := &dto.Metric{}
	require.NoError(t, pulsarMetric.Write(pulsarMetricDTO))
	assert.Equal(t, float64(1), pulsarMetricDTO.GetCounter().GetValue())
}

func TestRecordProducerMessage_NilProperties(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordProducerMessage("kafka", prometheusStatusSuccess, nil, 0)
	})
}
