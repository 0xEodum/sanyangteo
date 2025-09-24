.PHONY: help build up down logs test clean restart status logs-tg logs-mp

help:  ## Показать помощь
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

build:  ## Собрать образы
	docker-compose build

up:  ## Запустить сервисы
	docker-compose up -d

down:  ## Остановить сервисы
	docker-compose down

logs:  ## Показать логи всех сервисов
	docker-compose logs -f

logs-tg:  ## Показать логи tg-ingestor
	docker-compose logs -f tg-ingestor

logs-mp:  ## Показать логи message-preprocessor
	docker-compose logs -f message-preprocessor

test:  ## Тестировать Redis подключение
	docker exec tg-redis redis-cli ping

clean:  ## Очистить volumes и контейнеры
	docker-compose down -v
	docker system prune -f

restart: down up  ## Перезапустить

status:  ## Показать статус контейнеров
	docker-compose ps