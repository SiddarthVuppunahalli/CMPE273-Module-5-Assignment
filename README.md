# Order messaging lab (Go + RabbitMQ)

This repository builds an order producer and, in later checkpoints, independent
workers that consume from the same RabbitMQ queue.

## Checkpoint 1: broker and producer

Requirements: Go 1.22 or newer and Docker with Compose.

1. Start RabbitMQ:

   ```powershell
   docker compose up -d
   ```

2. Publish five orders from a separate terminal:

   ```powershell
   go run ./cmd/producer --count 5
   ```

The producer prints one `published` line per broker-confirmed message. Each
JSON message has `event_id`, `order_id`, `item_id`, and `quantity`. The durable
`orders` queue holds these messages until a worker is added in checkpoint 2.

Smoke test on October 26, 2026 (before any worker was started):

```text
published event_id=c3536fff543ee6334485b8e0c2dc4694 order_id=0767a8f7bf529459897650ed8fc82d20 item_id=item-001 quantity=1
published event_id=4be03aa85686c6fac4ce9f3ef3dbf94d order_id=9dd4c53ef0e3c612ab34ceaf0fc3120a item_id=item-001 quantity=1
published event_id=61ac996d52627c68954662c9e72315f7 order_id=35ee671f316f406746db323800a196cd item_id=item-001 quantity=1
published event_id=351f9d3c2467e3ceb3af3841089fbf0c order_id=a8aec7aff1ffa41f2e6d52baa3f03389 item_id=item-001 quantity=1
published event_id=3a00d67c01a5353087e7c97d09a08f2b order_id=fa6be468d6c3e01fde3c25c0dbef0301 item_id=item-001 quantity=1

$ docker compose exec -T rabbitmq rabbitmqctl list_queues name messages_ready messages_unacknowledged
name    messages_ready    messages_unacknowledged
orders  5                 0
```

RabbitMQ's management UI is available at <http://localhost:15672> with the
local lab credentials `orderlab` / `orderlab`. The producer uses
`amqp://orderlab:orderlab@127.0.0.1:5672/` by default; set `AMQP_URL` to use
another broker.

For a single event with a chosen ID (useful for the later duplicate demo):

```powershell
go run ./cmd/producer --count 1 --event-id demo-1 --order-id order-1 --item-id item-1 --quantity 2
```

To stop the broker, run `docker compose down`. This preserves its named volume
and queued messages. Run `docker compose down -v` only when you intentionally
want to reset the lab data.

## Architecture (planned)

```text
Order producer ──publish──> RabbitMQ orders queue ──deliver──> Worker A
                                              └────deliver──> Worker B
```

The workers, manual acknowledgements, redelivery demo, duplicate handling, and
captured evidence will be added in the next checkpoints.
