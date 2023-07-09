package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"Eyup-Devop/kafka-consumer/kafkaconsumer"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

var (
	server   *http.Server
	consumer *kafkaconsumer.KafkaConsumer
)

func main() {
	brokerList := []string{"127.0.0.1:19092"}
	topicList := []string{"example-topic"}

	consumer = kafkaconsumer.NewConsumer(strings.Join(brokerList, ","), topicList, "group1", nil)
	err := consumer.Init()
	if err != nil {
		log.Println(err)
		panic(err)
	}

	go ProduceTestMessages(brokerList, "group1", "example-topic")
	consumer.StartConsume()

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	go startHttpServer(":9090", httpHandle)

	<-sigchan
}

func ProduceTestMessages(br []string, grp string, topic string) {
	producer, _ := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": strings.Join(br, ",")})

	for i := 0; i < 10; i++ {
		time.Sleep(time.Second * 2)
		producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte("Example Message - " + string(rune(i))),
		}, nil)
	}
}

func startHttpServer(addr string, handler http.HandlerFunc) {
	http.HandleFunc("/", handler)
	log.Println("listening on", addr)
	server = &http.Server{
		Addr: addr,
	}
	server.ListenAndServe()
	log.Println("server stopped")
}

func httpHandle(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		consumer.Close()
		SendShutdownSignal()
		server.Shutdown(context.Background())
	}
}

func SendShutdownSignal() bool {
	pid := os.Getpid()
	process, err := os.FindProcess(pid)
	if err != nil {
		log.Println(err)
		return false
	}
	if err := process.Signal(os.Interrupt); err != nil {
		log.Println(err)
		return false
	}
	return true
}
