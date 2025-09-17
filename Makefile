.PHONY: help build up down logs test clean

help:  ## Показать помощь
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-10s\033[0m %s\n", $$1, $$2}'

build:  ## Собрать образы
	docker-compose build

up:  ## Запустить сервисы
	docker-compose up -d

down:  ## Остановить сервисы
	docker-compose down

logs:  ## Показать логи
	docker-compose logs -f tg-ingestor

test:  ## Тестировать Redis подключение
	docker exec tg-redis redis-cli ping

clean:  ## Очистить volumes и контейнеры
	docker-compose down -v
	docker system prune -f

restart: down up  ## Перезапустить

status:  ## Показать статус контейнеров
	docker-compose ps