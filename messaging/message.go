package messaging

import "time"

// IncomingMessage is a backend-neutral representation of a message consumed by the synchronizer server.
type IncomingMessage struct {
	ID         string
	Properties map[string]string
	Payload    []byte
}

// BuildProducerProperties builds the standard property map for outbound synchronizer server messages.
func BuildProducerProperties(account, cluster, eventType string, optionalProperties ...map[string]string) map[string]string {
	producerMessageProperties := map[string]string{
		MsgPropTimestamp:      time.Now().Format(time.RFC3339Nano),
		MsgPropEvent:          eventType,
		MsgPropProducerSource: MsgPropProducerSourceSynchronizerServer,
	}
	for _, optionalProperty := range optionalProperties {
		for k, v := range optionalProperty {
			producerMessageProperties[k] = v
		}
	}

	if account != "" {
		producerMessageProperties[MsgPropAccount] = account
	}

	if cluster != "" {
		producerMessageProperties[MsgPropCluster] = cluster
	}

	return producerMessageProperties
}

// IsSelfProducedMessage returns true if the message was produced by the synchronizer server.
func IsSelfProducedMessage(properties map[string]string, messageKey string) bool {
	if properties[MsgPropProducerSource] == MsgPropProducerSourceSynchronizerServer {
		return true
	}
	return messageKey == SynchronizerServerProducerKey
}
