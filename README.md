# Streams Exception Processor — Redis Streams → Postgres → Kafka

## What it does
- Reads Redis **Streams** (`exception.events`) using a **consumer group** (`exception-workers`).
- For each message with `securityId`, queries **all** matching rows from Postgres (paged) and publishes each to Kafka topic `exception-records`.
- **Async worker pool**: 4 threads/container. ACK to Redis only after async job finishes.
- Reclaimer uses `XCLAIM` (min-idle) to recover unacked messages from dead consumers.

## Run locally
1. Infra up: Redis 6379, Postgres 5432, Kafka 9092.
2. Configure `src/main/resources/application.yml` for creds/hosts.
3. `mvn -q -DskipTests spring-boot:run`
4. Seed sample rows:
   ```sql
   INSERT INTO exception_record(service_name, severity, message, occurred_at, security_id)
   VALUES ('order-service', 'HIGH', 'Err A', now() - interval '5 minutes', 'AAPL');
   INSERT INTO exception_record(service_name, severity, message, occurred_at, security_id)
   VALUES ('order-service', 'LOW', 'Err B', now() - interval '2 minutes', 'AAPL');
   ```
5. Trigger:
   ```bash
   redis-cli XADD exception.events MAXLEN ~ 1000000 * securityId AAPL
   ```
6. Observe logs: up to **4 concurrent** `proc-*` threads publishing to Kafka. Message is **ACKed after success**.
