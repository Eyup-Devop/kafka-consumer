package kafkaconsumer

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func NewConsumer(brokerList string, topicList []string, groupID string, writer MessageWriter) *KafkaConsumer {
	return &KafkaConsumer{
		brokerList: brokerList,
		topicList:  topicList,
		groupID:    groupID,
		writer:     writer,
		run:        true,
		consume:    false,
	}
}

type KafkaConsumer struct {
	consumer   *kafka.Consumer
	brokerList string
	topicList  []string
	groupID    string
	writer     MessageWriter
	run        bool
	consume    bool
}

func (kc *KafkaConsumer) Init() error {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":     kc.brokerList,
		"broker.address.family": "v4",
		"group.id":              kc.groupID,
		"auto.offset.reset":     "earliest",
	})
	if err != nil {
		return err
	}
	kc.consumer = c

	kc.consumer.SubscribeTopics(kc.topicList, nil)

	if kc.writer == nil {
		kc.writer = &DefaultMessageWriter{}
	}

	go kc.initConsume()

	return nil
}

func (kc *KafkaConsumer) initConsume() {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	for kc.run {
		for kc.consume {
			select {
			case sig := <-sigchan:
				fmt.Printf("Caught signal %v: terminating\n", sig)
				fmt.Println("Signal...............")
				kc.run = false
				kc.consume = false
			default:
				ev := kc.consumer.Poll(100)
				if ev == nil {
					continue
				}
				switch e := ev.(type) {
				case *kafka.Message:
					kc.writer.WriteMessage(e.TopicPartition.Topic, string(e.Value))
				case kafka.Error:
					// Errors should generally be considered
					// informational, the client will try to
					// automatically recover.
					fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
					if e.Code() == kafka.ErrAllBrokersDown {
						kc.run = false
						kc.consume = false
					}
				default:
					fmt.Printf("Ignored %v\n", e)
				}
			}
		}
	}
}

func (kc *KafkaConsumer) StartConsume() error {
	fmt.Println("Starting consumer")
	kc.consume = true
	return nil
}

func (kc *KafkaConsumer) StopConsume() error {
	fmt.Println("Stopping consumer")
	kc.consume = false
	return nil
}

func (kc *KafkaConsumer) Close() {
	fmt.Println("Closing consumer")
	kc.run = false
	kc.consume = false
	kc.consumer.Close()
}

func (kc *KafkaConsumer) Consumer() *kafka.Consumer {
	return kc.consumer
}

type MessageWriter interface {
	WriteMessage(*string, interface{}) error
}

type DefaultMessageWriter struct{}

func (d DefaultMessageWriter) WriteMessage(topic *string, message interface{}) error {
	fmt.Printf("Topic: %s, Message: %+v\n", *topic, message)
	return nil
}
