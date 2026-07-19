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

func TestNewFromConfig_ExplicitPulsarType(t *testing.T) {
	// An explicit type: "pulsar" selects Pulsar just like an absent type; with no
	// pulsarConfig it falls through to the mock (nil components, no error).
	components, err := newFromConfig(config.Config{
		Backend: config.Backend{
			MessageQueue: &config.MessageQueueConfig{Type: "pulsar"},
		},
	})
	require.NoError(t, err)
	assert.Nil(t, components)
}

func TestNewFromConfig_UnknownType(t *testing.T) {
	_, err := newFromConfig(config.Config{
		Backend: config.Backend{
			MessageQueue: &config.MessageQueueConfig{Type: "rabbitmq"},
		},
	})
	require.Error(t, err)
}

func TestNewFromConfig_KafkaValidation(t *testing.T) {
	tests := []struct {
		name  string
		kafka *config.KafkaConfig
	}{
		{name: "missing kafkaConfig", kafka: nil},
		{name: "missing bootstrapServers", kafka: &config.KafkaConfig{ProducerTopic: "out", ConsumerTopic: "in"}},
		{name: "missing producerTopic", kafka: &config.KafkaConfig{BootstrapServers: []string{"localhost:9092"}, ConsumerTopic: "in"}},
		{name: "missing consumerTopic", kafka: &config.KafkaConfig{BootstrapServers: []string{"localhost:9092"}, ProducerTopic: "out"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := newFromConfig(config.Config{
				Backend: config.Backend{
					MessageQueue: &config.MessageQueueConfig{Type: "kafka", KafkaConfig: tt.kafka},
				},
			})
			require.Error(t, err)
		})
	}
}
