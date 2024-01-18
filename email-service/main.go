package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/smtp"
	"time"

	"github.com/IBM/sarama"
)

type Order struct {
	ID     int    `json:"id"`
	Status string `json:"status"`
}

var (
	emailSent   = false
	messageRead = false
)

func main() {
	setupKafkaConsumer()
}

func setupKafkaConsumer() {
	config := sarama.NewConfig()
	consumer, err := sarama.NewConsumer([]string{"kafka:9092"}, config)
	if err != nil {
		log.Fatal("Error creating Kafka consumer:", err)
	}
	defer consumer.Close()

	log.Println("Kafka consumer connected successfully")

	partitionConsumer, err := consumer.ConsumePartition("orders", 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatal("Error consuming Kafka partition:", err)
	}

	log.Println("Kafka consumer is now consuming messages from the 'orders' topic")

	for {
		select {
		case msg := <-partitionConsumer.Messages():
			var order Order
			err := json.Unmarshal(msg.Value, &order)
			if err != nil {
				log.Println("Error unmarshaling order:", err)
				continue
			}

			if order.Status == "processed" {
				log.Printf("Processing Order ID: %d, Status: %s\n", order.ID, order.Status)

				sendEmailOnce(order)
			}

			messageRead = true

		case <-time.After(10 * time.Second):
			if !messageRead {
				log.Println("No new messages received.")
			}

			emailSent = false
			messageRead = false
		}
	}
}

func sendEmailOnce(order Order) {
	if !emailSent {
		sendEmail(order)
		emailSent = true
	}
}

func sendEmail(order Order) {
	to := "customer@example.com"
	subject := "Order Processed"

	// HTML and CSS template
	emailTemplate := `
	<!DOCTYPE html>
	<html lang="en">
	
	<head>
	    <meta charset="UTF-8">
	    <meta name="viewport" content="width=device-width, initial-scale=1.0">
	    <style>
	        body {
	            font-family: 'Arial', sans-serif;
	            background-color: #f4f4f4;
	            margin: 0;
	            padding: 0;
	            text-align: center;
	        }
	
	        .container {
	            max-width: 600px;
	            margin: 0 auto;
	            background-color: #fff;
	            padding: 20px;
	            border-radius: 10px;
	            box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
	            margin-top: 20px;
	        }
	
	        h1 {
	            color: #333;
	        }
	
	        p {
	            color: #555;
	        }
	
	        .cta-button {
	            display: inline-block;
	            padding: 10px 20px;
	            background-color: #007BFF;
	            color: #fff;
	            text-decoration: none;
	            border-radius: 5px;
	            margin-top: 20px;
	        }
	
	        .footer {
	            margin-top: 20px;
	            color: #777;
	        }
	    </style>
	</head>
	
	<body>
	    <div class="container">
	        <h1>Order Processed</h1>
	        <p>Your order with ID: <strong>{{.ID}}</strong> has been successfully processed.</p>
	        <a href="#" class="cta-button">View Details</a>
	    </div>
	
	    <div class="footer">
	        <p>This is an automated email. Please do not reply.</p>
	    </div>
	</body>
	
	</html>
	`

	// Create a new template and parse the HTML
	tmpl, err := template.New("emailTemplate").Parse(emailTemplate)
	if err != nil {
		log.Println("Error parsing email template:", err)
		return
	}

	// Create a buffer to store the rendered HTML
	var buffer bytes.Buffer

	// Execute the template, passing the order data
	err = tmpl.Execute(&buffer, order)
	if err != nil {
		log.Println("Error executing email template:", err)
		return
	}

	// Compose the email body
	emailBody := fmt.Sprintf("Subject: %s\n", subject)
	emailBody += "MIME-version: 1.0;\nContent-Type: text/html; charset=\"UTF-8\";\n\n"
	emailBody += buffer.String()

	// Email configuration
	emailUser := ""
	emailPassword := ""
	emailServer := "smtp.ethereal.email"
	emailPort := "587"

	// Send the email
	auth := smtp.PlainAuth("", emailUser, emailPassword, emailServer)
	err = smtp.SendMail(emailServer+":"+emailPort, auth, emailUser, []string{to}, []byte(emailBody))
	if err != nil {
		log.Println("Error sending email:", err)
	} else {
		log.Println("Email sent successfully")
	}
}
