package messaging

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	prometheusBackendLabel   = "backend"
	prometheusStatusLabel    = "status"
	prometheusKindLabel      = "kind"
	prometheusAccountLabel   = "account"
	prometheusClusterLabel   = "cluster"
	prometheusEventTypeLabel = "event_type"
	prometheusStatusSuccess  = "success"
	prometheusStatusError    = "error"
)

var (
	mqProducerMessagesProducedCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "synchronizer_mq_producer_messages_produced_count",
		Help: "The total number of messages produced to the message queue",
	}, []string{prometheusBackendLabel, prometheusStatusLabel, prometheusEventTypeLabel, prometheusKindLabel, prometheusAccountLabel, prometheusClusterLabel})
	mqProducerMessagePayloadBytesProducedCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "synchronizer_mq_producer_message_payload_bytes_produced_count",
		Help: "Counter of bytes published to the message queue (message payload)",
	}, []string{prometheusBackendLabel, prometheusStatusLabel, prometheusEventTypeLabel, prometheusKindLabel, prometheusAccountLabel, prometheusClusterLabel})
	pulsarProducerMessagesProducedCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "synchronizer_pulsar_producer_messages_produced_count",
		Help: "The total number of messages produced to pulsar",
	}, []string{prometheusStatusLabel, prometheusEventTypeLabel, prometheusKindLabel, prometheusAccountLabel, prometheusClusterLabel})
	pulsarProducerMessagePayloadBytesProducedCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "synchronizer_pulsar_producer_message_payload_bytes_produced_count",
		Help: "Counter of bytes published to pulsar (message payload) successfully",
	}, []string{prometheusStatusLabel, prometheusEventTypeLabel, prometheusKindLabel, prometheusAccountLabel, prometheusClusterLabel})
)

func producerMetricLabels(backend, status string, properties map[string]string) prometheus.Labels {
	labels := prometheus.Labels{
		prometheusBackendLabel:   backend,
		prometheusStatusLabel:    status,
		prometheusKindLabel:      "",
		prometheusClusterLabel:   "",
		prometheusAccountLabel:   "",
		prometheusEventTypeLabel: "",
	}
	if properties == nil {
		return labels
	}
	labels[prometheusKindLabel] = properties[MsgPropResourceKindResource]
	labels[prometheusClusterLabel] = properties[MsgPropCluster]
	labels[prometheusAccountLabel] = properties[MsgPropAccount]
	labels[prometheusEventTypeLabel] = properties[MsgPropEvent]
	return labels
}

// RecordProducerMessage increments backend-agnostic and Pulsar-alias producer metrics.
func RecordProducerMessage(backend, status string, properties map[string]string, payloadBytes int) {
	metricLabels := producerMetricLabels(backend, status, properties)
	mqProducerMessagesProducedCounter.With(metricLabels).Inc()
	mqProducerMessagePayloadBytesProducedCounter.With(metricLabels).Add(float64(payloadBytes))

	if backend == "pulsar" {
		pulsarLabels := prometheus.Labels{
			prometheusStatusLabel:    metricLabels[prometheusStatusLabel],
			prometheusKindLabel:      metricLabels[prometheusKindLabel],
			prometheusClusterLabel:   metricLabels[prometheusClusterLabel],
			prometheusAccountLabel:   metricLabels[prometheusAccountLabel],
			prometheusEventTypeLabel: metricLabels[prometheusEventTypeLabel],
		}
		pulsarProducerMessagesProducedCounter.With(pulsarLabels).Inc()
		pulsarProducerMessagePayloadBytesProducedCounter.With(pulsarLabels).Add(float64(payloadBytes))
	}
}
