# Order Status Service

Микросервис для отслеживания актуального статуса заказов.

Сервис использует:

- Go
- PostgreSQL
- NATS JetStream
- Redis
- Swagger
- Docker Compose

## Архитектура

POST /api/orders
        |
        +--> PostgreSQL
        |
        +--> NATS JetStream
                  |
                  v
             Pull Consumer
                  |
                  v
                Redis

JetStream хранит только последнее сообщение для каждого заказа.

Subject:

order.status.{order_id}

Пример:

order.status.order-100

## API

### Update order status

POST /api/orders

Request:

```json
{
  "order_id": "order-100",
  "status": "paid",
  "comment": "payment received"
}