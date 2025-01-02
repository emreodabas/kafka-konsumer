package kafka

import "sync/atomic"

type ConsumerMetric struct {
	totalUnprocessedMessagesCounter      int64
	totalProcessedMessagesCounter        int64
	totalErrorCountDuringFetchingMessage int64
}

func (cm *ConsumerMetric) IncrementTotalUnprocessedMessagesCounter(delta int64) {
	atomic.AddInt64(&cm.totalUnprocessedMessagesCounter, delta)
}

func (cm *ConsumerMetric) IncrementTotalProcessedMessagesCounter(delta int64) {
	atomic.AddInt64(&cm.totalProcessedMessagesCounter, delta)
}

func (cm *ConsumerMetric) IncrementTotalErrorCountDuringFetchingMessage(delta int64) {
	atomic.AddInt64(&cm.totalErrorCountDuringFetchingMessage, delta)
}
