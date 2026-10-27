# Order messaging lab (Go + RabbitMQ)

This repository builds an order producer and independent workers that consume
from the same RabbitMQ queue.

## Architecture

```mermaid
flowchart LR
    P[Order producer] --> Q[RabbitMQ orders queue]
    Q --> A[Worker A]
    Q --> B[Worker B]
    A --> S[Shared .order-state markers]
    B --> S
```

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

## Checkpoint 2: two competing workers

Start two independent worker processes in separate terminals:

```powershell
# Terminal A
go run ./cmd/worker --id worker-a --delay 2s
```

```powershell
# Terminal B
go run ./cmd/worker --id worker-b --delay 2s
```

After both print `ready`, publish another batch from a third terminal:

```powershell
go run ./cmd/producer --count 5
```

Each worker requests one unacknowledged message at a time. It logs `received`,
finishes the simulated order processing, and only then sends an ACK. Use Ctrl+C
to stop the workers after processing has finished.

This condensed log excerpt came from two separate worker processes consuming
the five messages published in checkpoint 1. The event IDs are distinct across
workers, and every `processed` line is followed by an `acked` line:

```text
worker-a:
18:48:39 received event_id=c3536fff543ee6334485b8e0c2dc4694 redelivered=false
18:48:41 processed event_id=c3536fff543ee6334485b8e0c2dc4694
18:48:41 acked event_id=c3536fff543ee6334485b8e0c2dc4694
18:48:41 received event_id=61ac996d52627c68954662c9e72315f7 redelivered=false
18:48:43 processed event_id=61ac996d52627c68954662c9e72315f7
18:48:43 acked event_id=61ac996d52627c68954662c9e72315f7
18:48:43 received event_id=3a00d67c01a5353087e7c97d09a08f2b redelivered=false
18:48:45 processed event_id=3a00d67c01a5353087e7c97d09a08f2b
18:48:45 acked event_id=3a00d67c01a5353087e7c97d09a08f2b

worker-b:
18:48:40 received event_id=4be03aa85686c6fac4ce9f3ef3dbf94d redelivered=false
18:48:42 processed event_id=4be03aa85686c6fac4ce9f3ef3dbf94d
18:48:42 acked event_id=4be03aa85686c6fac4ce9f3ef3dbf94d
18:48:42 received event_id=351f9d3c2467e3ceb3af3841089fbf0c redelivered=false
18:48:44 processed event_id=351f9d3c2467e3ceb3af3841089fbf0c
18:48:44 acked event_id=351f9d3c2467e3ceb3af3841089fbf0c

After both workers stopped, RabbitMQ reported:
$ docker compose exec -T rabbitmq rabbitmqctl list_queues name messages_ready messages_unacknowledged consumers
name    messages_ready    messages_unacknowledged    consumers
orders  0                 0                          0
```


![Two workers processing different orders](evidence/screenshots/competing-consumers.png)

*Screenshot from a later run: each worker processes different event IDs.*

## Checkpoint 3: failure before ACK and redelivery

Start with an empty queue and no workers. In terminal A, start the worker that
will intentionally exit after processing its first order but before ACK:

```powershell
go run ./cmd/worker --id worker-a --delay 25s --crash-before-ack
```

After it prints `ready`, publish one identifiable event from another terminal:

```powershell
go run ./cmd/producer --count 1 --event-id redelivery-demo-1 --order-id order-redelivery-1 --item-id item-001 --quantity 2
```

Once worker A prints `received`, start worker B in another terminal:

```powershell
go run ./cmd/worker --id worker-b --delay 1s
```

Worker A's nonzero exit is intentional. RabbitMQ requeues its unacknowledged
delivery, and worker B receives it. If B starts after A exits, it will still
receive the queued message. This is the captured output from the run:

```text
2026/10/26 19:05:19 worker=worker-a received event_id=redelivery-demo-1 order_id=order-redelivery-1 item_id=item-001 quantity=2 redelivered=false
2026/10/26 19:05:44 worker=worker-a processed event_id=redelivery-demo-1 order_id=order-redelivery-1
2026/10/26 19:05:44 worker=worker-a simulated_crash event_id=redelivery-demo-1 before_ack=true

2026/10/26 19:05:44 worker=worker-b received event_id=redelivery-demo-1 order_id=order-redelivery-1 item_id=item-001 quantity=2 redelivered=true
2026/10/26 19:05:45 worker=worker-b processed event_id=redelivery-demo-1 order_id=order-redelivery-1
2026/10/26 19:05:45 worker=worker-b acked event_id=redelivery-demo-1

$ docker compose ps --services --status running
rabbitmq
$ docker compose exec -T rabbitmq rabbitmqctl list_queues name messages_ready messages_unacknowledged consumers
name    messages_ready    messages_unacknowledged    consumers
orders  0                 0                          1
```

The broker stayed up, and worker B was still connected after ACK. These logs
were captured before duplicate handling was added, so both workers logged
`processed` for the same event. With the final worker, the same steps still
show `redelivered=true`, but B logs `duplicate` and skips processing.

![Worker failure followed by redelivery to another worker](evidence/screenshots/redelivery.png)

*Screenshot from a later run with duplicate handling enabled: worker A exits
before ACK, and worker B receives the redelivery and skips the duplicate.*

## Checkpoint 4: idempotent order handling

Run both workers from this repository folder so they share the default
`.order-state` directory. For workers launched from different folders, pass
the same absolute `--state-dir` path to each. Each marker file represents one
completed business action. Its name is a SHA-256 hash of `event_id`, and its
creation is atomic across worker processes. The marker is created after the
simulated work finishes and before ACK. A redelivery or a second publish with
the same `event_id` is logged as a duplicate and ACKed without another action.

To reproduce the test, use a new event ID. Start worker A, publish one order,
and start worker B after A logs `received`:

```powershell
# Terminal A
go run ./cmd/worker --id worker-a --delay 25s --crash-before-ack

# Producer terminal
go run ./cmd/producer --count 1 --event-id idempotency-demo-1 --order-id order-idempotency-1 --item-id item-002 --quantity 3

# Terminal B
go run ./cmd/worker --id worker-b --delay 1s
```

After B ACKs the redelivery, run the same producer command again. The captured
run shows exactly one `processed` action and two duplicate detections:

```text
2026/10/26 19:11:51 worker=worker-a received event_id=idempotency-demo-1 order_id=order-idempotency-1 item_id=item-002 quantity=3 redelivered=false
2026/10/26 19:12:16 worker=worker-a processed event_id=idempotency-demo-1 order_id=order-idempotency-1
2026/10/26 19:12:16 worker=worker-a simulated_crash event_id=idempotency-demo-1 before_ack=true

2026/10/26 19:12:16 worker=worker-b received event_id=idempotency-demo-1 order_id=order-idempotency-1 item_id=item-002 quantity=3 redelivered=true
2026/10/26 19:12:16 worker=worker-b duplicate event_id=idempotency-demo-1 skipped=true
2026/10/26 19:12:16 worker=worker-b acked event_id=idempotency-demo-1

2026/10/26 19:12:44 worker=worker-b received event_id=idempotency-demo-1 order_id=order-idempotency-1 item_id=item-002 quantity=3 redelivered=false
2026/10/26 19:12:44 worker=worker-b duplicate event_id=idempotency-demo-1 skipped=true
2026/10/26 19:12:44 worker=worker-b acked event_id=idempotency-demo-1

Processed marker files: 1
RabbitMQ orders queue: 0 ready, 0 unacknowledged
```

Marker files are local lab state and are intentionally ignored by Git. Use a
fresh `event_id` for another run or remove `.order-state` to reset the demo.

![Duplicate order skipped and acknowledged](evidence/screenshots/duplicate-handling.png)

*This screenshot reuses an already processed event ID, so both workers skip it.
The log excerpt above shows the original processing and later duplicate checks.*

## Reflection

RabbitMQ redelivers an unacknowledged message when a worker's connection
closes, so a crash after the work but before ACK can cause more than one
delivery. In the failure run, worker A processed the order and exited before
ACK, and worker B received the same `event_id` again. The worker checks a
shared marker so a repeated event is logged and ACKed without repeating the
lab's business action. For a real external side effect, the effect and its
idempotency record would need to be coordinated atomically.
