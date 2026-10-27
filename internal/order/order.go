package order

// QueueName is shared by the producer and workers.
const QueueName = "orders"

// Event is the JSON contract used on the orders queue.
type Event struct {
	EventID  string `json:"event_id"`
	OrderID  string `json:"order_id"`
	ItemID   string `json:"item_id"`
	Quantity int    `json:"quantity"`
}
