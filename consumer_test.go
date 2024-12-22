package kafka

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

func Test_consumer_startBatch(t *testing.T) {
	// Given
	var numberOfBatch int

	mc := mockReader{}
	c := consumer{
		base: &base{
			incomingMessageStream:  make(chan *IncomingMessage, 1),
			singleConsumingStream:  make(chan *Message, 1),
			messageProcessedStream: make(chan struct{}, 1),
			metric:                 &ConsumerMetric{},
			wg:                     sync.WaitGroup{},
			messageGroupDuration:   500 * time.Millisecond,
			r:                      &mc,
			concurrency:            3,
			logger:                 NewZapLogger(LogLevelDebug),
		},
		consumeFn: func(*Message) error {
			numberOfBatch++
			return nil
		},
	}

	go func() {
		// Simulate concurrency of value 3
		c.base.incomingMessageStream <- &IncomingMessage{
			kafkaMessage: &kafka.Message{},
			message:      &Message{},
		}
		c.base.incomingMessageStream <- &IncomingMessage{
			kafkaMessage: &kafka.Message{},
			message:      &Message{},
		}
		c.base.incomingMessageStream <- &IncomingMessage{
			kafkaMessage: &kafka.Message{},
			message:      &Message{},
		}

		time.Sleep(1 * time.Second)

		// Simulate messageGroupDuration
		c.base.incomingMessageStream <- &IncomingMessage{
			kafkaMessage: &kafka.Message{},
			message:      &Message{},
		}

		time.Sleep(1 * time.Second)

		// Return from startBatch
		close(c.base.incomingMessageStream)
	}()

	c.base.wg.Add(1 + c.base.concurrency)

	// When
	c.setupConcurrentWorkers()
	c.startBatch()

	// Then
	if numberOfBatch != 4 {
		t.Fatalf("Number of batch group must equal to 4")
	}

	if c.metric.TotalProcessedMessagesCounter != 4 {
		t.Fatalf("Total Processed Message Counter must equal to 4")
	}
}

func Test_consumer_process(t *testing.T) {
	t.Run("When_Processing_Is_Successful", func(t *testing.T) {
		// Given
		c := consumer{
			base: &base{metric: &ConsumerMetric{}},
			consumeFn: func(*Message) error {
				return nil
			},
		}

		// When
		c.process(&Message{})

		// Then
		if c.metric.TotalProcessedMessagesCounter != 1 {
			t.Fatalf("Total Processed Message Counter must equal to 3")
		}
		if c.metric.TotalUnprocessedMessagesCounter != 0 {
			t.Fatalf("Total Unprocessed Message Counter must equal to 0")
		}
	})
	t.Run("When_Re-processing_Is_Successful", func(t *testing.T) {
		// Given
		gotOnlyOneTimeException := true
		c := consumer{
			base: &base{metric: &ConsumerMetric{}, logger: NewZapLogger(LogLevelDebug), transactionalRetry: true},
			consumeFn: func(*Message) error {
				if gotOnlyOneTimeException {
					gotOnlyOneTimeException = false
					return errors.New("simulate only one time exception")
				}
				return nil
			},
		}

		// When
		c.process(&Message{})

		// Then
		if c.metric.TotalProcessedMessagesCounter != 1 {
			t.Fatalf("Total Processed Message Counter must equal to 1")
		}
		if c.metric.TotalUnprocessedMessagesCounter != 0 {
			t.Fatalf("Total Unprocessed Message Counter must equal to 0")
		}
	})
	t.Run("When_Re-processing_Is_Failed_And_Retry_Disabled", func(t *testing.T) {
		// Given
		c := consumer{
			base: &base{metric: &ConsumerMetric{}, logger: NewZapLogger(LogLevelDebug)},
			consumeFn: func(*Message) error {
				return errors.New("error case")
			},
		}

		// When
		c.process(&Message{})

		// Then
		if c.metric.TotalProcessedMessagesCounter != 0 {
			t.Fatalf("Total Processed Message Counter must equal to 0")
		}
		if c.metric.TotalUnprocessedMessagesCounter != 1 {
			t.Fatalf("Total Unprocessed Message Counter must equal to 1")
		}
	})
	t.Run("When_Re-processing_Is_Failed_And_Retry_Enabled", func(t *testing.T) {
		// Given
		mc := mockCronsumer{}
		c := consumer{
			base: &base{metric: &ConsumerMetric{}, logger: NewZapLogger(LogLevelDebug), retryEnabled: true, cronsumer: &mc},
			consumeFn: func(*Message) error {
				return errors.New("error case")
			},
		}

		// When
		c.process(&Message{})

		// Then
		if c.metric.TotalProcessedMessagesCounter != 0 {
			t.Fatalf("Total Processed Message Counter must equal to 0")
		}
		if c.metric.TotalUnprocessedMessagesCounter != 1 {
			t.Fatalf("Total Unprocessed Message Counter must equal to 1")
		}
	})
	t.Run("When_Re-processing_Is_Failed_And_Retry_Failed", func(t *testing.T) {
		// Given
		mc := mockCronsumer{wantErr: true}
		c := consumer{
			base: &base{metric: &ConsumerMetric{}, logger: NewZapLogger(LogLevelDebug), retryEnabled: true, cronsumer: &mc},
			consumeFn: func(*Message) error {
				return errors.New("error case")
			},
		}

		// When
		c.process(&Message{})

		// Then
		if c.metric.TotalProcessedMessagesCounter != 0 {
			t.Fatalf("Total Processed Message Counter must equal to 0")
		}
		if c.metric.TotalUnprocessedMessagesCounter != 1 {
			t.Fatalf("Total Unprocessed Message Counter must equal to 1")
		}
	})
}

func Test_consumer_Pause(t *testing.T) {
	// Given
	ctx, cancelFn := context.WithCancel(context.Background())
	c := consumer{
		base: &base{
			logger:  NewZapLogger(LogLevelDebug),
			pause:   make(chan struct{}),
			context: ctx, cancelFn: cancelFn,
			consumerState: stateRunning,
		},
	}
	go func() {
		<-c.base.pause
	}()

	// When
	c.Pause()

	// Then
	if c.base.consumerState != statePaused {
		t.Fatal("consumer state must be in paused")
	}
}

func Test_consumer_Resume(t *testing.T) {
	// Given
	mc := mockReader{}
	ctx, cancelFn := context.WithCancel(context.Background())
	c := consumer{
		base: &base{
			r:       &mc,
			logger:  NewZapLogger(LogLevelDebug),
			pause:   make(chan struct{}),
			quit:    make(chan struct{}),
			wg:      sync.WaitGroup{},
			context: ctx, cancelFn: cancelFn,
		},
	}

	// When
	c.Resume()

	// Then
	if c.base.consumerState != stateRunning {
		t.Fatal("consumer state must be in running")
	}
}
