"""
Factory для создания queue writers и readers
"""

from typing import Dict, Any

from .interfaces import QueueType, QueueWriter, QueueReader, QueueConfigError
from .strategies.redis_strategy import RedisQueueWriter, RedisQueueReader


class QueueFactory:
    """Factory для создания queue clients"""
    
    @staticmethod
    def create_writer(queue_type: QueueType, config: Dict[str, Any]) -> QueueWriter:
        """
        Создать QueueWriter для указанного типа очереди
        
        Args:
            queue_type: Тип очереди
            config: Конфигурация подключения
            
        Returns:
            Экземпляр QueueWriter
            
        Raises:
            QueueConfigError: Если конфигурация некорректна
        """
        if queue_type == QueueType.REDIS:
            return QueueFactory._create_redis_writer(config)
        elif queue_type == QueueType.KAFKA:
            return QueueFactory._create_kafka_writer(config)
        elif queue_type == QueueType.RABBITMQ:
            return QueueFactory._create_rabbitmq_writer(config)
        else:
            raise QueueConfigError(f"Unsupported queue type: {queue_type}")
    
    @staticmethod
    def create_reader(queue_type: QueueType, config: Dict[str, Any]) -> QueueReader:
        """
        Создать QueueReader для указанного типа очереди
        
        Args:
            queue_type: Тип очереди
            config: Конфигурация подключения
            
        Returns:
            Экземпляр QueueReader
        """
        if queue_type == QueueType.REDIS:
            return QueueFactory._create_redis_reader(config)
        elif queue_type == QueueType.KAFKA:
            return QueueFactory._create_kafka_reader(config)
        elif queue_type == QueueType.RABBITMQ:
            return QueueFactory._create_rabbitmq_reader(config)
        else:
            raise QueueConfigError(f"Unsupported queue type: {queue_type}")
    
    @staticmethod
    def _create_redis_writer(config: Dict[str, Any]) -> RedisQueueWriter:
        """Создать Redis writer"""
        required_fields = ["url"]
        QueueFactory._validate_config(config, required_fields, "Redis writer")
        
        return RedisQueueWriter(
            url=config["url"],
            dedup_ttl_seconds=config.get("dedup_ttl_seconds", 604800),
            maxlen_approx=config.get("maxlen_approx", 1_000_000),
            max_connections=config.get("max_connections", 10),
            socket_timeout=config.get("socket_timeout", 5.0),
            socket_connect_timeout=config.get("socket_connect_timeout", 5.0)
        )
    
    @staticmethod
    def _create_redis_reader(config: Dict[str, Any]) -> RedisQueueReader:
        """Создать Redis reader"""
        required_fields = ["url"]
        QueueFactory._validate_config(config, required_fields, "Redis reader")
        
        return RedisQueueReader(
            url=config["url"],
            max_connections=config.get("max_connections", 10),
            socket_timeout=config.get("socket_timeout", 5.0)
        )
    
    @staticmethod
    def _create_kafka_writer(config: Dict[str, Any]) -> QueueWriter:
        """Создать Kafka writer (заглушка для будущей реализации)"""
        required_fields = ["bootstrap_servers"]
        QueueFactory._validate_config(config, required_fields, "Kafka writer")
        
        # TODO: Реализовать KafkaQueueWriter
        raise NotImplementedError("Kafka writer not implemented yet")
    
    @staticmethod
    def _create_kafka_reader(config: Dict[str, Any]) -> QueueReader:
        """Создать Kafka reader (заглушка для будущей реализации)"""
        required_fields = ["bootstrap_servers"]
        QueueFactory._validate_config(config, required_fields, "Kafka reader")
        
        # TODO: Реализовать KafkaQueueReader
        raise NotImplementedError("Kafka reader not implemented yet")
    
    @staticmethod
    def _create_rabbitmq_writer(config: Dict[str, Any]) -> QueueWriter:
        """Создать RabbitMQ writer (заглушка для будущей реализации)"""
        required_fields = ["url"]
        QueueFactory._validate_config(config, required_fields, "RabbitMQ writer")
        
        # TODO: Реализовать RabbitMQQueueWriter
        raise NotImplementedError("RabbitMQ writer not implemented yet")
    
    @staticmethod
    def _create_rabbitmq_reader(config: Dict[str, Any]) -> QueueReader:
        """Создать RabbitMQ reader (заглушка для будущей реализации)"""
        required_fields = ["url"]
        QueueFactory._validate_config(config, required_fields, "RabbitMQ reader")
        
        # TODO: Реализовать RabbitMQQueueReader
        raise NotImplementedError("RabbitMQ reader not implemented yet")
    
    @staticmethod
    def _validate_config(
        config: Dict[str, Any], 
        required_fields: list, 
        component_name: str
    ) -> None:
        """
        Валидировать конфигурацию
        
        Args:
            config: Конфигурация для валидации
            required_fields: Список обязательных полей
            component_name: Имя компонента для ошибок
            
        Raises:
            QueueConfigError: Если конфигурация некорректна
        """
        missing_fields = [
            field for field in required_fields 
            if field not in config or config[field] is None
        ]
        
        if missing_fields:
            raise QueueConfigError(
                f"{component_name} missing required fields: {missing_fields}"
            )
        
        # Дополнительная валидация для Redis URL
        if "url" in config and config["url"]:
            url = config["url"]
            if not url.startswith(("redis://", "rediss://")):
                raise QueueConfigError(
                    f"{component_name} invalid Redis URL format: {url}"
                )