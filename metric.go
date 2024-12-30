package kafka

import "sync/atomic"

type ConsumerMetric struct {
	totalUnprocessedMessagesCounter      int64
	totalProcessedMessagesCounter        int64
	totalErrorCountDuringFetchingMessage int64
}

func (cm *ConsumerMetric) IncrementTotalUnprocessedMessagesCounter() {
	atomic.AddInt64(&cm.totalUnprocessedMessagesCounter, 1)
}

func (cm *ConsumerMetric) IncrementTotalProcessedMessagesCounter() {
	atomic.AddInt64(&cm.totalProcessedMessagesCounter, 1)
}

func (cm *ConsumerMetric) IncrementTotalErrorCountDuringFetchingMessage() {
	atomic.AddInt64(&cm.totalErrorCountDuringFetchingMessage, 1)
}
