package kafka

import (
	"fmt"
	"github.com/gofiber/fiber/v2/log"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/segmentio/kafka-go"

	kcronsumer "github.com/Trendyol/kafka-cronsumer/pkg/kafka"
)

type consumer struct {
	*base

	consumeFn func(*Message) error
}

func (c *consumer) Pause() {
	c.base.Pause()
}

func (c *consumer) Resume() {
	c.base.Resume()
}

func newSingleConsumer(cfg *ConsumerConfig) (Consumer, error) {
	consumerBase, err := newBase(cfg, cfg.Concurrency)
	if err != nil {
		return nil, err
	}

	c := consumer{
		base:      consumerBase,
		consumeFn: cfg.ConsumeFn,
	}

	if cfg.RetryEnabled {
		c.base.setupCronsumer(cfg, func(message kcronsumer.Message) error {
			return c.consumeFn(toMessage(message))
		})
	}

	if cfg.APIEnabled {
		c.base.setupAPI(cfg, c.metric)
	}

	return &c, nil
}

func (c *consumer) GetMetricCollectors() []prometheus.Collector {
	return c.base.GetMetricCollectors()
}

func (c *consumer) Consume() {
	go c.subprocesses.Start()

	c.wg.Add(1)
	go c.startConsume()

	c.setupConcurrentWorkers()

	c.wg.Add(1)
	go c.startBatch()
}

func (c *consumer) startBatch() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.messageGroupDuration)
	defer ticker.Stop()

	messages := make([]*Message, 0, c.concurrency)
	commitMessages := make([]kafka.Message, 0, c.concurrency)

	for {
		select {
		case <-ticker.C:
			if len(messages) == 0 {
				continue
			}

			c.consume(&messages, &commitMessages)
		case msg, ok := <-c.incomingMessageStream:
			if !ok {
				close(c.singleConsumingStream)
				close(c.messageProcessedStream)
				return
			}

			messages = append(messages, msg.message)
			commitMessages = append(commitMessages, *msg.kafkaMessage)

			if len(messages) == c.concurrency {
				c.consume(&messages, &commitMessages)
			}
		}
	}
}

func (c *consumer) setupConcurrentWorkers() {
	for i := 0; i < c.concurrency; i++ {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			for message := range c.singleConsumingStream {
				c.process(message)
				c.messageProcessedStream <- struct{}{}
			}
		}()
	}
}

func (c *consumer) consume(messages *[]*Message, commitMessages *[]kafka.Message) {
	messageList := *messages

	// Send the messages to process
	for _, message := range messageList {
		c.singleConsumingStream <- message
	}

	// Wait the messages to be processed
	for range messageList {
		<-c.messageProcessedStream
	}

	if err := c.r.CommitMessages(*commitMessages); err != nil {
		c.logger.Errorf("Commit Error %s,", err.Error())
	}

	// Clearing resources
	*commitMessages = (*commitMessages)[:0]
	*messages = (*messages)[:0]
}

func (c *consumer) process(message *Message) {
	consumeErr := c.consumeFn(message)

	if consumeErr != nil {
		if c.transactionalRetry {
			c.logger.Warnf("Consume Function Err %s, Message will be retried", consumeErr.Error())
			// Try to process same message again
			if consumeErr = c.consumeFn(message); consumeErr != nil {
				c.logger.Warnf("Consume Function Again Err %s, message is sending to exception/retry topic %s", consumeErr.Error(), c.retryTopic)
				c.metric.TotalUnprocessedMessagesCounter++
			}
		} else {
			c.metric.TotalUnprocessedMessagesCounter++
		}
	}

	if consumeErr != nil && c.retryEnabled {
		retryableMsg := message.toRetryableMessage(c.retryTopic, consumeErr.Error())
		if err := c.retryWithBackoff(retryableMsg); err != nil {
			errorMessage := fmt.Sprintf(
				"Error producing message %s to exception/retry topic %s. Error: %s",
				string(message.Value), c.retryTopic, err.Error())
			c.logger.Error(errorMessage)
			panic(err.Error())
		}
	}

	if consumeErr == nil {
		c.metric.TotalProcessedMessagesCounter++
	}
}

func (c *consumer) retryWithBackoff(retryableMsg kcronsumer.Message) error {
	var produceErr error

	for attempt := 1; attempt <= 5; attempt++ {
		produceErr = c.cronsumer.Produce(retryableMsg)
		if produceErr == nil {
			return nil
		}
		log.Warnf("Error producing message (attempt %d/%d): %v", attempt, 5, produceErr)
		time.Sleep((50 * time.Millisecond) * time.Duration(1<<attempt))
	}

	return produceErr
}
