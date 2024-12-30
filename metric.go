package kafka

import (
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

func (m *ConsumerMetric) IncrementTotalUnprocessedMessagesCounter() {
	atomic.AddInt64(&m.TotalUnprocessedMessagesCounter, 1)
}

func (m *ConsumerMetric) IncrementTotalProcessedMessagesCounter() {
	atomic.AddInt64(&m.TotalProcessedMessagesCounter, 1)
}

func (m *ConsumerMetric) IncrementTotalErrorCountDuringFetchingMessage() {
	atomic.AddInt64(&m.TotalErrorCountDuringFetchingMessage, 1)
}
