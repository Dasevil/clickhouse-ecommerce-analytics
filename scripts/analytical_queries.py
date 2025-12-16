from clickhouse_driver import Client

client = Client(host='localhost', port=9000, user='default', password='', database='default')

def top_categories():
    print("Топ-20 категорий по количеству товаров:")
    result = client.execute('''
        SELECT category_id, countMerge(offer_count) as cnt
        FROM ecom.catalog_by_category_mv
        GROUP BY category_id
        ORDER BY cnt DESC
        LIMIT 20
    ''')
    for row in result:
        print(row)

def top_brands():
    print("\nТоп-30 брендов по количеству товаров:")
    result = client.execute('''
        SELECT vendor, countMerge(offer_count) as cnt
        FROM ecom.catalog_by_brand_mv
        GROUP BY vendor
        ORDER BY cnt DESC
        LIMIT 30
    ''')
    for row in result:
        print(row)

def avg_products_per_brand():
    print("\nСреднее количество товаров по брендам в категориях:")
    result = client.execute('''
        SELECT category_id, avg(offer_count) as avg_offers
        FROM (
            SELECT category_id, vendor, sum(countMerge(offer_count)) as offer_count
            FROM ecom.catalog_by_category_mv
            GROUP BY category_id, vendor
        )
        GROUP BY category_id
        ORDER BY avg_offers DESC
        LIMIT 20
    ''')
    for row in result:
        print(row)

def uncovered_offers():
    print("\nТовары без пользовательских событий:")
    result = client.execute('''
        SELECT count(*) as uncovered_count
        FROM ecom.ecom_offers
        WHERE offer_id NOT IN (
            SELECT DISTINCT ContentUnitID
            FROM ecom.raw_events
        )
    ''')
    print(f"Товаров без событий: {result[0][0]}")

if __name__ == '__main__':
    top_categories()
    top_brands()
    avg_products_per_brand()
    uncovered_offers()