#!/bin/bash

# Запуск Docker-контейнеров
docker-compose up -d

# Ожидание готовности ClickHouse
echo "Ожидание готовности ClickHouse..."
sleep 30

# Инициализация схемы данных
echo "Инициализация схемы ClickHouse..."
clickhouse-client --host=localhost --port=9000 < scripts/init.sql

# Загрузка данных (если доступны)
if [ -d "data/parquet" ]; then
    echo "Загрузка данных в ClickHouse..."
    python scripts/load_data.py
fi

# Запуск аналитических запросов
echo "Выполнение аналитических запросов..."
python scripts/analytical_queries.py

# Тестирование производительности
echo "Тестирование производительности..."
python scripts/performance_test.py

echo "Настройка завершена! Grafana доступна по адресу: http://localhost:3000 (логин: admin, пароль: admin)"