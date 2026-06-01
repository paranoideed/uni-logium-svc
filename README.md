# uni-logium-svc

Сервіс логування подій. Споживає повідомлення з AWS SQS, які генерує Debezium CDC з `uni-products-svc`. Підтримує механізм ретраїв для обробки транзієнтних помилок.

## Налаштування

### Змінні середовища

```bash
cp .example.env .env
```

Заповніть `.env`:

```env
AWS_ACCESS_KEY_ID=<ваш-access-key>
AWS_SECRET_ACCESS_KEY=<ваш-secret-key>
AWS_REGION=eu-north-1
SQS_QUEUE_URL=https://sqs.eu-north-1.amazonaws.com/<account-id>/products-events
```

### Конфігурація SQS (`config.yaml`)

```yaml
sqs:
  queue_url: "${SQS_QUEUE_URL}"
  workers: 10
  fetchers: 10
  visibility_timeout: 29s
```

`queue_url` підтягується з `.env`.

## Запуск

```bash
# Спільна мережа (один раз, якщо ще не створена)
docker network create uni-net

docker compose up -d
```

## Механізм ретраїв

Сервіс симулює транзієнтні помилки (~10% запитів) для демонстрації retry-логіки:

```go
if rand.IntN(10) == 0 {
    // симульована помилка → повідомлення повертається в чергу для повторної обробки
    return fmt.Errorf("simulated transient error")
}
```

Події обробляються незалежно одна від одної (без гарантії порядку) — для сервісу логів це природна поведінка.
