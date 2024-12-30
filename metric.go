package kafka

import (
	"github.com/prometheus/client_golang/prometheus"
	"strconv"
	"sync"
	"sync/atomic"
)

type ConsumerMetric struct {
	TotalUnprocessedMessagesCounter              int64
	TotalProcessedMessagesCounter                int64
	TotalErrorCountDuringFetchingMessage         int64
	mu                                           sync.Mutex
	TotalErrorDuringProducingToRetryTopicCounter map[string]map[string]float64
}

func InitConsumerMetrics() *ConsumerMetric {
	return &ConsumerMetric{
		TotalErrorDuringProducingToRetryTopicCounter: make(map[string]map[string]float64),
	}
}

func (m *ConsumerMetric) IncrementTotalErrorDuringProducingToRetryTopicCounter(topic string, partition int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.TotalErrorDuringProducingToRetryTopicCounter[topic]; !exists {
		m.TotalErrorDuringProducingToRetryTopicCounter[topic] = make(map[string]float64)
	}
	m.TotalErrorDuringProducingToRetryTopicCounter[topic][strconv.Itoa(partition)]++
}

func (m *ConsumerMetric) CollectMetrics(vec *prometheus.CounterVec) {
	for topic, partitions := range m.TotalErrorDuringProducingToRetryTopicCounter {
		for partition, count := range partitions {
			vec.With(prometheus.Labels{
				"topic":     topic,
				"partition": partition,
			}).Add(count)
		}
	}
}

func (m *ConsumerMetric) IncrementTotalUnprocessedMessagesCounter() {
	atomic.AddInt64(&m.TotalUnprocessedMessagesCounter, 1)
}

func (m *ConsumerMetric) IncrementTotalProcessedMessagesCounter() {
	atomic.AddInt64(&m.TotalProcessedMessagesCounter, 1)
}

func (m *ConsumerMetric) IncrementTotalErrorCountDuringFetchingMessage() {
	atomic.AddInt64(&m.TotalErrorCountDuringFetchingMessage, 1)
}
