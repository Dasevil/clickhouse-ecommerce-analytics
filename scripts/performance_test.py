import time
from clickhouse_driver import Client

client = Client(host='localhost', port=9000, user='default', password='', database='default')

def measure_query_time(query, use_mv):
    start = time.time()
    client.execute(query)
    end = time.time()
    print(f"{'MV' if use_mv else 'Raw'} query time: {end - start:.4f} seconds")
    return end - start

# Запрос через материализованное представление
mv_query = '''
    SELECT category_id, countMerge(offer_count) as cnt
    FROM ecom.catalog_by_category_mv
    GROUP BY category_id
    ORDER BY cnt DESC
    LIMIT 20
'''

# Запрос по сырым данным
raw_query = '''
    SELECT category_id, count(*) as cnt
    FROM ecom.ecom_offers
    GROUP BY category_id
    ORDER BY cnt DESC
    LIMIT 20
'''

print("Сравнение производительности: МВ vs Сырые данные\n")

# Прогрев
client.execute(mv_query)
client.execute(raw_query)

# Измерения
mv_time = measure_query_time(mv_query, use_mv=True)
raw_time = measure_query_time(raw_query, use_mv=False)

speedup = raw_time / mv_time if mv_time > 0 else float('inf')
print(f"\nУскорение: {speedup:.2f}x")