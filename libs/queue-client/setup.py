"""
Setup script для queue-client библиотеки
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="queue-client",
    version="1.0.0",
    author="YuDev",
    description="Unified queue client library for Redis, Kafka, RabbitMQ",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
    install_requires=[
        "redis[hiredis]>=4.6.0",
        "pydantic>=2.5.0",
    ],
    extras_require={
        "kafka": [
            "kafka-python>=2.0.0",
            "aiokafka>=0.8.0",
        ],
        "rabbitmq": [
            "aio-pika>=9.0.0",
        ],
        "all": [
            "kafka-python>=2.0.0",
            "aiokafka>=0.8.0",
            "aio-pika>=9.0.0",
        ],
        "dev": [
            "pytest>=7.4.0",
            "pytest-asyncio>=0.21.0",
            "pytest-cov>=4.1.0",
            "black>=23.0.0",
            "isort>=5.12.0",
            "mypy>=1.7.0",
            "flake8>=6.1.0",
        ]
    },
    include_package_data=True,
    zip_safe=False,
)