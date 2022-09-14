package main

import (
	"fmt"
	"listener-service/event"
	"log"
	"math"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	// connet rabbitmq
	rabbitConnection, err := connect()
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	defer rabbitConnection.Close()


	//create consumer
	log.Println("Listening for and consuming rabbitmq messages...!")
	consumer, err := event.NewConsumer(rabbitConnection)
	if err != nil {
		panic(err)
	}

	// watch the queue
	err = consumer.Listen([]string{"log.INFO", "log.WARNING", "log.ERROR"})
	if err != nil {
		log.Println(err)
	}
}

func connect() (*amqp.Connection, error) {
	var counts int64
	var backOff = 1 * time.Second
	var connection *amqp.Connection

	for {
		c, err := amqp.Dial("amqp://guest:guest@localhost")
		if err != nil {
			fmt.Println("Rabbitmq not yet ready")
			counts++
		} else {
			log.Println("Connected to rabbitmq!")
			connection = c
			break
		}

		if counts > 5 {
			fmt.Println(err)
			return nil, err
		}

		backOff = time.Duration(math.Pow(float64(counts), 2)) * time.Second
		log.Println("backing off...")
		time.Sleep(backOff)
		continue
	}

	return connection, nil
}