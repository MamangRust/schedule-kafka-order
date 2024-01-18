package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

type Order struct {
	ID     int    `json:"id"`
	Status string `json:"status"`
}

func main() {
	setupKafkaConsumer()
}

var wg sync.WaitGroup

func setupKafkaConsumer() {
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumer, err := sarama.NewConsumerGroup([]string{"kafka:9092"}, "my-group", config)
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	// Create a channel to signal when the consumer is closed
	closedCh := make(chan struct{})

	go func() {
		defer wg.Done()
		for {
			err := consumer.Consume(context.Background(), []string{"orders"}, &orderConsumer{closedCh: closedCh})
			if err != nil {
				log.Println("Error from consumer:", err)
				// Check for errors and try to recover
				recoverConsumer(&consumer, closedCh)
			}
		}
	}()

	log.Println("Kafka consumer is now consuming messages from the 'orders' topic")

	// Wait for signals
	select {
	case <-sigterm:
		log.Println("Interrupt is detected")
	case <-closedCh:
		log.Println("Consumer closed")
	}

	// Wait for the consumer goroutine to finish
	wg.Wait()
}

func recoverConsumer(consumer *sarama.ConsumerGroup, closedCh chan struct{}) {
	log.Println("Attempting to recover the consumer...")
	time.Sleep(5 * time.Second) // Wait for a while before retrying

	// Check if the consumer is still open
	if (*consumer).Close() != nil {
		log.Println("Consumer is already closed. Cannot recover.")
		return
	}

	// Create a new consumer and replace the existing one
	newConsumer, err := sarama.NewConsumerGroup([]string{"kafka:9092"}, "order-consumer-group", sarama.NewConfig())
	if err != nil {
		log.Println("Error creating a new consumer:", err)
		return
	}

	// Replace the existing consumer with the new one
	(*consumer).Close()
	consumer = &newConsumer

	log.Println("Consumer recovered successfully.")
}

type orderConsumer struct {
	closedCh chan struct{}
}

func (oc *orderConsumer) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (oc *orderConsumer) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (oc *orderConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		var order Order
		err := json.Unmarshal(msg.Value, &order)
		if err != nil {
			log.Println("Error unmarshaling order:", err)
			continue
		}

		if order.Status == "processed" {
			processOrder(order)
		}

		session.MarkMessage(msg, "")
	}

	// Notify that the consumer has finished consuming
	oc.closedCh <- struct{}{}

	return nil
}

func processOrder(order Order) {
	fmt.Println("Processing order: ", order.Status)
	log.Printf("Processing Order ID: %d, Status: %s\n", order.ID, order.Status)
}
