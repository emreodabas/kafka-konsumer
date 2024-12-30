package kafka

type ConsumerMetric struct {
	totalUnprocessedMessagesCounter      int64
	totalProcessedMessagesCounter        int64
	totalErrorCountDuringFetchingMessage int64
}

func (cm *ConsumerMetric) IncrementTotalUnprocessedMessagesCounter() {
	cm.totalUnprocessedMessagesCounter++
}

func (cm *ConsumerMetric) IncrementTotalProcessedMessagesCounter() {
	cm.totalProcessedMessagesCounter++
}

func (cm *ConsumerMetric) IncrementTotalErrorCountDuringFetchingMessage() {
	cm.totalErrorCountDuringFetchingMessage++
}
