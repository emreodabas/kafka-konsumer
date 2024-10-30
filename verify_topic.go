package kafka

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

type kafkaClient interface {
	Metadata(ctx context.Context, req *kafka.MetadataRequest) (*kafka.MetadataResponse, error)
	GetClient() *kafka.Client
}

type client struct {
	*kafka.Client
}

func newKafkaClient(cfg *ConsumerConfig) (kafkaClient, error) {
	var err error
	kc := &client{
		Client: &kafka.Client{
			Addr: kafka.TCP(cfg.Reader.Brokers...),
		},
	}

	transport := &Transport{
		Transport: &kafka.Transport{
			MetadataTopics: cfg.getTopics(),
		},
	}
	if err = fillLayer(transport, cfg.SASL, cfg.TLS); err != nil {
		err = fmt.Errorf("error when initializing kafka client for verify topic purpose %w", err)
		return nil, err
	}

	kc.Transport = transport

	if err != nil {
		return nil, err
	}

	return kc, nil
}

func (k *client) GetClient() *kafka.Client {
	return k.Client
}

func verifyTopics(client kafkaClient, cfg *ConsumerConfig) (bool, error) {
	topics := cfg.getTopics()

	metadata, err := client.Metadata(context.Background(), &kafka.MetadataRequest{
		Topics: topics,
	})
	if err != nil {
		return false, fmt.Errorf("error when during verifyTopics metadata request %w", err)
	}
	return checkTopicsWithinMetadata(metadata, topics)
}

func checkTopicsWithinMetadata(metadata *kafka.MetadataResponse, topics []string) (bool, error) {
	metadataTopics := make(map[string]struct{}, len(metadata.Topics))
	for _, topic := range metadata.Topics {
		if topic.Error != nil {
			continue
		}
		metadataTopics[topic.Name] = struct{}{}
	}

	for _, topic := range topics {
		if _, exist := metadataTopics[topic]; !exist {
			return false, nil
		}
	}
	return true, nil
}
