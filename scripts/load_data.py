import os
import pandas as pd
from clickhouse_driver import Client

# Клиент ClickHouse
client = Client(host='localhost', port=9000, user='default', password='', database='default')

# Пути к данным
DATA_DIR = 'C:/Users/User/PycharmProjects/sql/data/parquet'

# Загрузка каталога товаров
offers_file = os.path.join(DATA_DIR, 'ecom_offers.csv')
if os.path.exists(offers_file):
    offers_df = pd.read_csv(offers_file)
else:
    offers_df = pd.read_parquet(os.path.join(DATA_DIR, 'ecom_offers.parquet'))
client.execute(
    'INSERT INTO ecom.ecom_offers (offer_id, price, seller_id, category_id, vendor) VALUES',
    offers_df.to_dict('records')
)

# Загрузка событий пользователей
events_df = pd.read_parquet(os.path.join(DATA_DIR, 'raw_events.parquet'))
client.execute(
    'INSERT INTO ecom.raw_events (Hour, DeviceTypeName, ApplicationName, OSName, ProvinceName, ContentUnitID) VALUES',
    events_df.to_dict('records')
)

print("Данные успешно загружены!")