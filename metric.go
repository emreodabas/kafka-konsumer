package kafka

import "sync/atomic"

type ConsumerMetric struct {
	TotalUnprocessedMessagesCounter      int64
	TotalProcessedMessagesCounter        int64
	TotalErrorCountDuringFetchingMessage int64
}

func (cm *ConsumerMetric) IncrementTotalUnprocessedMessagesCounter() {
	atomic.AddInt64(&cm.TotalUnprocessedMessagesCounter, 1)
}

func (cm *ConsumerMetric) IncrementTotalProcessedMessagesCounter() {
	atomic.AddInt64(&cm.TotalProcessedMessagesCounter, 1)
}

func (cm *ConsumerMetric) IncrementTotalErrorCountDuringFetchingMessage() {
	atomic.AddInt64(&cm.TotalErrorCountDuringFetchingMessage, 1)
}
