package main

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/IBM/sarama"
)

type Order struct {
	ID     int    `json:"id"`
	Status string `json:"status"`
}

var kafkaProducer sarama.SyncProducer

func main() {
	setupKafkaProducer()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Order Service"))
	})
	http.HandleFunc("/placeOrder", placeOrderHandler)
	// go scheduleCheckOrderStatus()

	log.Fatal(http.ListenAndServe(":5000", nil))
}

func setupKafkaProducer() {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer([]string{"kafka:9092"}, config)
	if err != nil {
		log.Fatal("Error creating Kafka producer:", err)
	}

	kafkaProducer = producer
	log.Println("Kafka producer connected successfully")
}

func placeOrderHandler(w http.ResponseWriter, r *http.Request) {
	var order Order
	err := json.NewDecoder(r.Body).Decode(&order)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	publishOrderToKafka(order)

	w.WriteHeader(http.StatusOK)

	w.Write([]byte("Order placed successfully"))
}

func publishOrderToKafka(order Order) {
	message, err := json.Marshal(order)
	if err != nil {
		log.Println("Error marshaling order:", err)
		return
	}

	_, _, err = kafkaProducer.SendMessage(&sarama.ProducerMessage{
		Topic: "orders",
		Value: sarama.StringEncoder(message),
	})
	if err != nil {
		log.Println("Error sending message to Kafka:", err)
	}
}

func scheduleCheckOrderStatus() {
	ticker := time.NewTicker(5 * time.Minute)

	for range ticker.C {
		processedOrder := Order{ID: 1, Status: "processed"}
		publishOrderToKafka(processedOrder)
	}
}
