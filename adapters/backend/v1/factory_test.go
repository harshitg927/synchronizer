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

// validKafkaConfig returns an otherwise-valid Kafka config with one field mutated,
// so each test case isolates the single setting under test.
func validKafkaConfig(mutate func(*config.KafkaConfig)) *config.KafkaConfig {
	kafkaCfg := &config.KafkaConfig{
		BootstrapServers: []string{"localhost:9092"},
		ProducerTopic:    "out",
		ConsumerTopic:    "in",
	}
	mutate(kafkaCfg)
	return kafkaCfg
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
		// Security settings must be rejected while enforcement is unimplemented.
		{name: "unsupported securityProtocol", kafka: validKafkaConfig(func(k *config.KafkaConfig) { k.SecurityProtocol = "SASL_SSL" })},
		{name: "tls enabled", kafka: validKafkaConfig(func(k *config.KafkaConfig) { k.TLSEnabled = true })},
		{name: "tls ca cert set", kafka: validKafkaConfig(func(k *config.KafkaConfig) { k.TLSCaCertPath = "/etc/kafka/ca.crt" })},
		{name: "sasl mechanism set", kafka: validKafkaConfig(func(k *config.KafkaConfig) { k.SASLMechanism = "SCRAM-SHA-256" })},
		{name: "sasl username set", kafka: validKafkaConfig(func(k *config.KafkaConfig) { k.SASLUsername = "svc" })},
		{name: "sasl password set", kafka: validKafkaConfig(func(k *config.KafkaConfig) { k.SASLPassword = "hunter2" })},
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
