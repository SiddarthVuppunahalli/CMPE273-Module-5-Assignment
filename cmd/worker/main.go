package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
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
	workerID := flag.String("id", fmt.Sprintf("worker-%d", os.Getpid()), "name shown in logs")
	delay := flag.Duration("delay", time.Second, "simulated processing time per order")
	crashBeforeAck := flag.Bool("crash-before-ack", false, "exit on the first order after processing but before ACK (demo only)")
	stateDir := flag.String("state-dir", ".order-state", "shared directory for processed event markers")
	flag.Parse()
	if strings.TrimSpace(*workerID) == "" || strings.TrimSpace(*stateDir) == "" || *delay < 0 {
		return errors.New("id and state-dir must be nonempty and delay must not be negative")
	}
	statePath, err := filepath.Abs(*stateDir)
	if err != nil {
		return fmt.Errorf("resolve state directory: %w", err)
	}
	if err := os.MkdirAll(statePath, 0700); err != nil {
		return fmt.Errorf("create state directory: %w", err)
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
	if err := ch.Qos(1, 0, false); err != nil {
		return fmt.Errorf("set prefetch: %w", err)
	}
	deliveries, err := ch.Consume(queue.Name, *workerID, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("start consumer: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log.Printf("worker=%s ready queue=%s state_dir=%s", *workerID, queue.Name, statePath)
	for {
		select {
		case <-ctx.Done():
			log.Printf("worker=%s stopping", *workerID)
			return nil
		case delivery, ok := <-deliveries:
			if !ok {
				return errors.New("RabbitMQ delivery channel closed")
			}
			if err := handle(*workerID, *delay, *crashBeforeAck, statePath, delivery); err != nil {
				return err
			}
		}
	}
}

func handle(workerID string, delay time.Duration, crashBeforeAck bool, stateDir string, delivery amqp.Delivery) error {
	var event order.Event
	if err := json.Unmarshal(delivery.Body, &event); err != nil || event.EventID == "" || event.OrderID == "" || event.ItemID == "" || event.Quantity < 1 {
		log.Printf("worker=%s rejected invalid order: %s", workerID, delivery.Body)
		if err := delivery.Reject(false); err != nil {
			return fmt.Errorf("reject invalid order: %w", err)
		}
		return nil
	}

	log.Printf("worker=%s received event_id=%s order_id=%s item_id=%s quantity=%d redelivered=%t", workerID, event.EventID, event.OrderID, event.ItemID, event.Quantity, delivery.Redelivered)
	processed, err := processOnce(stateDir, event.EventID, delay)
	if err != nil {
		return fmt.Errorf("process order %s: %w", event.EventID, err)
	}
	if processed {
		log.Printf("worker=%s processed event_id=%s order_id=%s", workerID, event.EventID, event.OrderID)
	} else {
		log.Printf("worker=%s duplicate event_id=%s skipped=true", workerID, event.EventID)
	}
	if crashBeforeAck {
		log.Printf("worker=%s simulated_crash event_id=%s before_ack=true", workerID, event.EventID)
		os.Exit(2) // Abrupt exit leaves the delivery unacknowledged for RabbitMQ to requeue.
	}
	if err := delivery.Ack(false); err != nil {
		return fmt.Errorf("acknowledge order %s: %w", event.EventID, err)
	}
	log.Printf("worker=%s acked event_id=%s", workerID, event.EventID)
	return nil
}

// Creating the marker is this lab's business action. O_EXCL makes it atomic
// across worker processes, so only one worker can perform it for an event ID.
func processOnce(stateDir, eventID string, delay time.Duration) (bool, error) {
	marker := filepath.Join(stateDir, fmt.Sprintf("%x.done", sha256.Sum256([]byte(eventID))))
	if _, err := os.Stat(marker); err == nil {
		return false, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}

	time.Sleep(delay) // Simulate work before committing the business action.
	file, err := os.OpenFile(marker, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if errors.Is(err, os.ErrExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if err := file.Close(); err != nil {
		return false, err
	}
	return true, nil
}
