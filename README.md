# Order Status Service

Микросервис для отслеживания актуального статуса заказов.

Сервис реализован на Go и использует PostgreSQL, NATS JetStream и Redis.

## Технологии

- Go
- PostgreSQL
- NATS JetStream
- Redis
- Docker Compose
- Swagger

## Архитектура

```text
POST /api/orders
        |
        +----> PostgreSQL
        |
        +----> NATS JetStream
                   |
                   v
              Pull Consumer
                   |
                   v
                 Redis
```

PostgreSQL хранит актуальное состояние заказа.

NATS JetStream используется как основной источник сообщений о статусах.

Redis используется как быстрый кэш.

JetStream хранит только последнее сообщение для каждого заказа.

Subject:

```text
order.status.{order_id}
```

Пример:

```text
order.status.order-100
```

## API

### Update order status

```http
POST /api/orders
```

Request:

```json
{
  "order_id": "order-100",
  "status": "paid",
  "comment": "payment received"
}
```

Допустимые статусы:

```text
created
paid
shipped
delivered
```

Response:

```json
{
  "order_id": "order-100",
  "updated_at": "2026-10-03T15:17:47.358068+04:00"
}
```

При обновлении существующего заказа используется PostgreSQL UPSERT.

После сохранения статус публикуется в NATS JetStream.

---

### Get current order status

```http
GET /api/orders/{order_id}
```

Пример:

```http
GET /api/orders/order-100
```

Response:

```json
{
  "order_id": "order-100",
  "status": "shipped",
  "comment": "final update",
  "updated_at": "2026-10-02T20:09:53.640695+04:00"
}
```

Если заказ не найден:

```text
404 Not Found
```

Данные читаются из PostgreSQL.

---

### Get order status directly from JetStream

```http
GET /api/stream/orders/{order_id}
```

Пример:

```http
GET /api/stream/orders/order-100
```

Response:

```json
{
  "status": "shipped",
  "comment": "final update",
  "seq": 12,
  "timestamp": "2026-10-02T16:09:53.643188761Z"
}
```

Используется JetStream Direct Get.

Consumer для этого запроса не создаётся.

Если сообщение отсутствует:

```text
404 Not Found
```

---

### Synchronize Redis cache

```http
POST /api/admin/sync-cache
```

Роут принудительно восстанавливает Redis-кэш из последних сообщений JetStream.

Response:

```json
{
  "synced": 2
}
```

## NATS JetStream

Stream:

```text
ORDERS
```

Subjects:

```text
order.status.*
```

Основные настройки:

```text
MaxMsgsPerSubject = 1
AllowDirect = true
Storage = FileStorage
```

Это означает, что для каждого заказа хранится только последнее сообщение.

Например после 10 обновлений:

```text
order.status.order-100
```

в stream остаётся только одно актуальное сообщение.

При публикации используется `MsgId`, сформированный через SHA-256 от `order_id` и времени обновления.

## Pull Consumer

Consumer используется для обновления Redis.

Настройки:

```text
AckPolicy = Explicit
MaxDeliver = 3
FilterSubject = order.status.*
```

Периодически consumer получает новые сообщения:

```text
JetStream
    |
    v
Pull Consumer
    |
    v
Redis SET
    |
    v
Ack
```

При ошибке записи в Redis выполняется повторная попытка через 10 секунд.

Максимальное количество попыток:

```text
3
```

## Redis

Формат ключа:

```text
order:{order_id}:status
```

Пример:

```text
order:order-100:status
```

Значение хранится в JSON.

Пример:

```json
{
  "order_id": "order-100",
  "status": "delivered",
  "comment": "delivered to customer",
  "updated_at": "2026-10-03T12:59:11.664696+04:00"
}
```

TTL:

```text
1 hour
```

## PostgreSQL

Используется база:

```text
orders
```

Основная таблица:

```text
orders
```

Поля:

```text
order_id
status
comment
updated_at
```

`order_id` является Primary Key.

## Docker Compose

Через Docker Compose запускаются:

```text
orders-postgres
orders-nats
orders-redis
```

Порты:

```text
PostgreSQL : 5436
NATS       : 4222
NATS HTTP  : 8222
Redis      : 6379
```

## Запуск инфраструктуры

```bash
cd docker-compose
docker compose up -d
```

Проверить:

```bash
docker ps
```

## Миграция PostgreSQL

Из корня проекта:

```bash
docker exec -i orders-postgres \
  psql -U orders -d orders < migrations/001_init.sql
```

## Запуск приложения

Из корня проекта:

```bash
go run ./cmd/api
```

Пример успешного запуска:

```text
PostgreSQL connected
NATS JetStream connected
Redis connected
JetStream consumer created
server started on :8080
order consumer started
```

## Swagger

После запуска приложения Swagger UI доступен по адресу:

```text
http://localhost:8080/swagger/index.html
```

Swagger содержит:

```text
POST /api/orders
GET  /api/orders/{order_id}
GET  /api/stream/orders/{order_id}
POST /api/admin/sync-cache
```

Для обновления Swagger-документации:

```bash
swag init -g cmd/api/main.go
```

## Graceful Shutdown

Приложение обрабатывает:

```text
Ctrl+C
SIGTERM
```

При завершении:

```text
shutting down...
order consumer stopped
consumer stopped
server stopped
```

HTTP server корректно завершается, consumer останавливается, а соединения с NATS, Redis и PostgreSQL закрываются.

## Проверка проекта

```bash
go fmt ./...
go vet ./...
go test ./...
```

## Структура проекта

```text
practiceNATSJetstream/
├── cmd/
│   └── api/
│       └── main.go
├── internal/
│   ├── cache/
│   ├── handler/
│   ├── model/
│   ├── nats/
│   ├── repository/
│   └── worker/
├── migrations/
│   └── 001_init.sql
├── docker-compose/
│   ├── docker-compose.yml
│   └── nats.conf
├── docs/
├── .env.example
├── .gitignore
├── README.md
├── go.mod
└── go.sum
```