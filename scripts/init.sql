CREATE DATABASE IF NOT EXISTS ecom;

-- Таблица каталога товаров
CREATE TABLE IF NOT EXISTS ecom.ecom_offers (
    offer_id String,
    price Float64,
    seller_id String,
    category_id UInt32,
    vendor String
) ENGINE = ReplacingMergeTree()
ORDER BY (category_id, offer_id)
PARTITION BY category_id;

-- Таблица пользовательских событий
CREATE TABLE IF NOT EXISTS ecom.raw_events (
    Hour DateTime,
    DeviceTypeName String,
    ApplicationName String,
    OSName String,
    ProvinceName String,
    ContentUnitID String
) ENGINE = MergeTree()
ORDER BY (Hour, ContentUnitID)
PARTITION BY toYYYYMM(Hour);

-- Материализованное представление: агрегация по категориям
CREATE MATERIALIZED VIEW IF NOT EXISTS ecom.catalog_by_category_mv
ENGINE = AggregatingMergeTree()
ORDER BY category_id
PARTITION BY intDiv(category_id, 1000)
AS SELECT
    category_id,
    countState() AS offer_count,
    sumState(price) AS total_price,
    uniqState(seller_id) AS unique_sellers
FROM ecom.ecom_offers
GROUP BY category_id;

-- Материализованное представление: агрегация по брендам
CREATE MATERIALIZED VIEW IF NOT EXISTS ecom.catalog_by_brand_mv
ENGINE = AggregatingMergeTree()
ORDER BY vendor
PARTITION BY substring(vendor, 1, 1)
AS SELECT
    vendor,
    countState() AS offer_count,
    uniqState(category_id) AS categories_count,
    avgState(price) AS avg_price
FROM ecom.ecom_offers
GROUP BY vendor;

-- Материализованное представление: анализ покрытия каталога событиями
CREATE MATERIALIZED VIEW IF NOT EXISTS ecom.catalog_coverage_mv
ENGINE = AggregatingMergeTree()
ORDER BY offer_id
AS SELECT
    e.offer_id,
    countState() AS event_count,
    uniqState(toDate(Hour)) AS active_days
FROM ecom.ecom_offers e
LEFT JOIN ecom.raw_events r ON e.offer_id = r.ContentUnitID
GROUP BY e.offer_id;