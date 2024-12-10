package kafka

import (
	"github.com/prometheus/client_golang/prometheus"
	"strconv"
	"sync"
)

type ConsumerMetric struct {
	TotalUnprocessedMessagesCounter              int64
	TotalProcessedMessagesCounter                int64
	TotalErrorCountDuringFetchingMessage         int64
	TotalErrorDuringProducingToRetryTopicCounter map[string]map[string]float64
	mutex                                        sync.Mutex
}

func InitConsumerMetrics() *ConsumerMetric {
	return &ConsumerMetric{
		TotalErrorDuringProducingToRetryTopicCounter: make(map[string]map[string]float64),
	}
}

func (m *ConsumerMetric) IncrementTotalErrorDuringProducingToRetryTopicCounter(topic string, partition int) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if _, exists := m.TotalErrorDuringProducingToRetryTopicCounter[topic]; !exists {
		m.TotalErrorDuringProducingToRetryTopicCounter[topic] = make(map[string]float64)
	}
	m.TotalErrorDuringProducingToRetryTopicCounter[topic][strconv.Itoa(partition)]++
}

func (m *ConsumerMetric) CollectMetrics(vec *prometheus.CounterVec) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	for topic, partitions := range m.TotalErrorDuringProducingToRetryTopicCounter {
		for partition, count := range partitions {
			vec.With(prometheus.Labels{
				"topic":     topic,
				"partition": partition,
			}).Add(count)
		}
	}
}
