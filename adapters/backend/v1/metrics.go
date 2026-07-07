package backend

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	prometheusAccountLabel = "account"
	prometheusClusterLabel = "cluster"
)

var (
	connectedClientsGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "synchronizer_connected_clients_count",
		Help: "The number of connected clients",
	})
	clientDisconnectionCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "synchronizer_client_disconnection_count",
		Help: "Counter of client disconnections",
	}, []string{prometheusAccountLabel, prometheusClusterLabel})
)
