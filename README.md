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
        v
   NATS JetStream ----> Pull Consumer ----> Redis
        |
        v
    PostgreSQL
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
Idempotency-Key: unique-request-key
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

Порядок обновления: валидация → idempotency claim → подготовка события → публикация в
JetStream → запись заказа в PostgreSQL и завершение idempotency → ответ.
Один timestamp создаётся в Go (UTC, точность до микросекунд) и сохраняется в событии,
таблицах `orders` / `idempotency_keys` и HTTP-ответе.

`Idempotency-Key` обязателен (до 256 байт). Повтор с тем же ключом и тем же
логическим JSON возвращает первоначальный `updated_at` без нового события.
Порядок полей JSON и пробелы не влияют на SHA-256 request hash; неизвестные поля
и несколько JSON-объектов в одном запросе отклоняются. Другой body с тем же ключом — 409.
Параллельный запрос с занятым ключом также получает 409 и может повториться позже.
`order_id` во всех HTTP handlers соответствует `^[A-Za-z0-9_-]{1,64}$`.

Idempotency использует PostgreSQL session advisory lock для одной активной попытки
и уникальный индекс для одного незавершённого обновления заказа. В таблице остаются:

- `updated_at` и `expected_sequence`: сохранённое намерение публикации;
- `published_sequence`: подтверждённая публикация;
- `completed`: заказ и результат записаны одной SQL-транзакцией.

До сохранения намерения ошибка освобождает claim. После него запись сохраняется:
повтор того же ключа и body продолжает обработку. При ошибке PostgreSQL после
публикации повтор выполняет только оставшуюся SQL-часть. При потере PubAck или
ошибке записи `published_sequence` публикация распознаётся по MsgId и точному
содержимому последнего сообщения. Сохранённый expected sequence защищает от
повторного события даже после истечения окна дедупликации NATS.

Пока операция незавершена, другой ключ для этого заказа получает 409. Это сохраняет
последнее событие для восстановления. Восстановление запускается повтором клиента;
фонового процесса восстановления нет. При внешней записи напрямую в NATS,
удалении/пересоздании stream или ручном изменении таблиц может потребоваться
сверка состояния оператором. Таблицу idempotency нельзя очищать без согласованной
политики срока жизни ключей. Между JetStream и PostgreSQL нет общей транзакции:
временно событие может быть видно в NATS/Redis до успешной записи `orders`.

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

При публикации используется `MsgId` — SHA-256 от `Idempotency-Key`, а также
`WithExpectLastSequencePerSubject` с ожидаемым sequence, сохранённым до публикации.

## Pull Consumer

Consumer используется для обновления Redis.

Настройки:

```text
AckPolicy = Explicit
MaxDeliver = 3
FilterSubject = order.status.*
```

Consumer сразу обрабатывает backlog через `FetchNoWait(100)` до пустого батча,
затем проверяет новые сообщения каждую секунду:

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

При ошибке записи в Redis вызывается `NakWithDelay(10 * time.Second)`.
Повтор доставляет JetStream; ручного retry-цикла нет. Невалидный JSON завершается
через `Term()`. После третьей неудачной доставки сообщение также завершается.

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
cp .env.example .env # заполнить локальные значения
docker compose --env-file .env -f docker-compose/docker-compose.yml up -d
```

Проверить:

```bash
docker ps
```

## Миграция PostgreSQL

Из корня проекта:

```bash
docker compose --env-file .env -f docker-compose/docker-compose.yml exec -T postgres \
  sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < migrations/created_orders_tables.sql
docker compose --env-file .env -f docker-compose/docker-compose.yml exec -T postgres \
  sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < migrations/idempotency_keys.sql
```

## Запуск приложения

Из корня проекта:

```bash
set -a
source .env
set +a
go run ./cmd/api
```

`.env` не загружается приложением автоматически и исключён из Git.
При обновлении старой схемы миграция сохраняет завершённые ключи, но остановится,
если есть старые `completed=false` записи без сохранённого намерения. Их нужно
сначала сверить с JetStream; повторять такие операции вслепую небезопасно.

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
│   ├── created_orders_tables.sql
│   └── idempotency_keys.sql
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

## Интеграционные тесты

Unit-тесты запускаются через `go test ./...`. Для PostgreSQL-тестов задать
`TEST_POSTGRES_URL`: каждый тест создаёт отдельную схему и удаляет только её.
Для NATS-тестов задать `TEST_NATS_URL`, указывающий на отдельный тестовый сервер
с JetStream (тесты создают/настраивают `ORDERS`).

```bash
TEST_POSTGRES_URL="$POSTGRES_URL" TEST_NATS_URL="nats://127.0.0.1:14222" go test -race ./...
```

Проверяются атомарный claim, одинаковые/конфликтующие/параллельные запросы,
отказ подготовки, ошибка и неопределённый результат publish, сбои SQL после
публикации, откат order UPSERT при сбое завершения, CAS, NATS Drain и retry worker.
