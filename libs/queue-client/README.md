# Queue Client Library

Унифицированная библиотека для работы с очередями. Поддерживает Redis Streams, Kafka, RabbitMQ через единый интерфейс.

## Возможности

- 🔄 **Единый интерфейс** для разных типов очередей
- 🛡️ **Идемпотентность** - защита от дублирования сообщений  
- 🔌 **Легкая смена технологий** - изменение конфигурации без изменения кода
- 📊 **Structured logging** с метриками
- 🏥 **Health checks** для мониторинга
- ⚡ **Async/await** поддержка
- 🎯 **Type hints** для лучшего IDE experience

## Быстрый старт

### Установка

```bash
# Базовая установка (Redis)
pip install -e libs/queue-client

# С поддержкой Kafka
pip install -e "libs/queue-client[kafka]"

# С поддержкой всех очередей
pip install -e "libs/queue-client[all]"
```

### Использование

#### Запись в очередь

```python
from queue_client import create_queue_client, QueueMessage

# Создаем клиент
client = await create_queue_client("redis", {
    "url": "redis://localhost:6379/0"
})

# Отправляем сообщение
message = QueueMessage(
    key="user:123:action",
    data={"user_id": 123, "action": "login", "timestamp": "2024-01-01T12:00:00Z"},
    headers={"service": "auth", "version": "1.0"}
)

result = await client.write("user_events", message)
print(f"Message ID: {result.message_id}, Duplicate: {result.is_duplicate}")

await client.close()
```

#### Чтение из очереди

```python
from queue_client import create_queue_client

client = await create_queue_client("redis", {
    "url": "redis://localhost:6379/0"
})

# Читаем сообщения
async for message in client.read("user_events", "my_consumer_group"):
    print(f"Received: {message.data}")
    
    # Обрабатываем сообщение
    await process_message(message.data)
    
    # Подтверждаем обработку
    await client.ack("user_events", message.message_id)

await client.close()
```

#### Context Manager

```python
from queue_client import create_queue_client, QueueMessage

async with await create_queue_client("redis", {"url": "redis://localhost"}) as client:
    result = await client.write("events", QueueMessage("key", {"data": "value"}))
    print(f"Written: {result.message_id}")
```

## Конфигурация

### Redis

```python
config = {
    "url": "redis://localhost:6379/0",
    "dedup_ttl_seconds": 604800,  # 7 дней
    "maxlen_approx": 1000000,     # Макс. размер stream
    "max_connections": 10,
    "socket_timeout": 5.0
}
```

### Kafka (планируется)

```python
config = {
    "bootstrap_servers": "localhost:9092",
    "security_protocol": "PLAINTEXT",
    "acks": "all",
    "retries": 3
}
```

## Миграция с HTTP Gateway

### До (HTTP Gateway)

```python
# В каждом сервисе нужен HTTP клиент
import httpx

async with httpx.AsyncClient() as client:
    response = await client.post(
        "http://queue-writer:8080/events",
        json=event_data,
        headers={"Authorization": f"Bearer {token}"}
    )
```

### После (Shared Library)

```python
# Единый интерфейс во всех сервисах
from queue_client import create_queue_client, QueueMessage

client = await create_queue_client("redis", {"url": "redis://redis:6379"})
result = await client.write("events", QueueMessage(key, data))
```

## Смена технологии

Для смены Redis на Kafka нужно изменить только конфигурацию:

```python
# Было
config = {
    "type": "redis",
    "url": "redis://localhost:6379"
}

# Стало
config = {
    "type": "kafka", 
    "bootstrap_servers": "localhost:9092"
}
```

Код остается неизменным! 🎉

## Архитектура

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Service   │    │    Queue    │    │    Redis    │
│             │    │   Client    │    │   Kafka     │
│ write(data) │───▶│  (Strategy) │───▶│  RabbitMQ   │
│             │    │             │    │             │
└─────────────┘    └─────────────┘    └─────────────┘
```

### Strategy Pattern

- `RedisQueueWriter` - для Redis Streams
- `KafkaQueueWriter` - для Kafka (планируется)  
- `RabbitMQQueueWriter` - для RabbitMQ (планируется)

## Идемпотентность

Библиотека автоматически обеспечивает защиту от дублирования:

```python
# Отправляем одно и то же сообщение дважды
message = QueueMessage("same_key", {"data": "value"})

result1 = await client.write("queue", message)  # success=True, is_duplicate=False
result2 = await client.write("queue", message)  # success=True, is_duplicate=True
```

## Health Checks

```python
health = await client.health_check()
print(health)
# {
#   "connected": True,
#   "writer_healthy": True,
#   "reader_healthy": True,
#   "overall_healthy": True
# }
```

## Логирование

Библиотека использует structured logging:

```json
{
  "timestamp": "2024-01-01T12:00:00Z",
  "level": "INFO",
  "component": "redis_queue_writer",
  "message": "Message written to stream",
  "queue_name": "events",
  "stream_id": "1704110400000-0",
  "message_key": "user:123:login"
}
```

## Разработка

```bash
# Установка зависимостей разработки
pip install -e "libs/queue-client[dev]"

# Запуск тестов
pytest libs/queue-client/tests/

# Линтинг
black libs/queue-client/
isort libs/queue-client/
flake8 libs/queue-client/
mypy libs/queue-client/
```

## Roadmap

- ✅ Redis Streams поддержка
- ⏳ Kafka поддержка
- ⏳ RabbitMQ поддержка
- ⏳ Метрики и трейсинг
- ⏳ Circuit breaker pattern
- ⏳ Retry policies