package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/SiddarthVuppunahalli/CMPE273-Module-5-Assignment/internal/order"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	count := flag.Int("count", 5, "number of orders to publish")
	eventID := flag.String("event-id", "", "event ID for a single order")
	orderID := flag.String("order-id", "", "order ID for a single order")
	itemID := flag.String("item-id", "item-001", "item ID")
	quantity := flag.Int("quantity", 1, "quantity per order")
	flag.Parse()

	if *count < 1 || *quantity < 1 || *itemID == "" {
		return errors.New("count, quantity, and item-id must be positive/nonempty")
	}
	if *count != 1 && (*eventID != "" || *orderID != "") {
		return errors.New("event-id and order-id can only be set with --count 1")
	}

	url := os.Getenv("AMQP_URL")
	if url == "" {
		url = "amqp://orderlab:orderlab@127.0.0.1:5672/"
	}
	conn, err := amqp.Dial(url)
	if err != nil {
		return fmt.Errorf("connect to RabbitMQ: %w", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}
	defer ch.Close()

	queue, err := ch.QueueDeclare(order.QueueName, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("declare queue: %w", err)
	}
	if err := ch.Confirm(false); err != nil {
		return fmt.Errorf("enable publisher confirms: %w", err)
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	for i := 0; i < *count; i++ {
		event := order.Event{ItemID: *itemID, Quantity: *quantity}
		event.EventID, err = idOrRandom(*eventID)
		if err != nil {
			return fmt.Errorf("generate event ID: %w", err)
		}
		event.OrderID, err = idOrRandom(*orderID)
		if err != nil {
			return fmt.Errorf("generate order ID: %w", err)
		}
		body, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("encode order: %w", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err = ch.PublishWithContext(ctx, "", queue.Name, true, false, amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			MessageId:    event.EventID,
			Body:         body,
		})
		if err != nil {
			cancel()
			return fmt.Errorf("publish order %s: %w", event.EventID, err)
		}
		select {
		case confirm, ok := <-confirms:
			if !ok || !confirm.Ack {
				cancel()
				return fmt.Errorf("broker did not confirm order %s", event.EventID)
			}
		case <-ctx.Done():
			cancel()
			return fmt.Errorf("wait for broker confirmation of %s: %w", event.EventID, ctx.Err())
		}
		cancel()
		log.Printf("published event_id=%s order_id=%s item_id=%s quantity=%d", event.EventID, event.OrderID, event.ItemID, event.Quantity)
	}
	return nil
}

func idOrRandom(id string) (string, error) {
	if id != "" {
		return id, nil
	}
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes[:]), nil
}
