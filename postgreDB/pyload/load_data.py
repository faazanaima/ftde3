import os
import pandas as pd
from sqlalchemy import create_engine

# Dapatkan URL database dari variabel lingkungan
database_url = os.getenv('DATABASE_URL')

if not database_url:
    raise ValueError("DATABASE_URL environment variable is not set")

print(f"Connecting to database at {database_url}")

# Buat engine untuk koneksi ke database PostgreSQL
engine = create_engine(database_url)

# Daftar file CSV dan nama tabel target
csv_files = {
    'categories.csv': 'categories',
    'customers.csv': 'customers',
    'employee_territories.csv': 'employee_territories',
    'employees.csv': 'employees',
    'order_details.csv': 'order_details',
    'orders.csv': 'orders',
    'products.csv': 'products',
    'regions.csv': 'regions',
    'shippers.csv': 'shippers',
    'suppliers.csv': 'suppliers',
    'territories.csv': 'territories',
}

# Loop untuk membaca setiap file CSV dan memuatnya ke tabel PostgreSQL
for file_name, table_name in csv_files.items():
    try:
        print(f"Loading {file_name} into {table_name}")
        df = pd.read_csv(f'data/{file_name}')
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f'{file_name} berhasil dimuat ke tabel {table_name}')
    except Exception as e:
        print(f"Error loading {file_name} into {table_name}: {e}")