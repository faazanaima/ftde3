from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG
dag = DAG(
    'postgres_to_bigquery',
    default_args=default_args,
    description='Extract data from PostgreSQL, process it, and load into BigQuery',
    schedule_interval='0 0 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

def extract_data_from_postgres(table_name, **kwargs):
    import psycopg2
    import pandas as pd

    conn = psycopg2.connect(
        dbname='air',
        user='postgres',
        password='123',
        host='localhost',
    )

    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, conn)
    df.to_csv(f'/tmp/{table_name}.csv', index=False)

    conn.close()

table_names = [
    'categories', 'customers', 'employee_territories', 'employees', 'order_details',
    'orders', 'products', 'regions', 'shippers', 'suppliers', 'territories'
]

extract_tasks = []
for table in table_names:
    task = PythonOperator(
        task_id=f'extract_{table}',
        python_callable=extract_data_from_postgres,
        op_kwargs={'table_name': table},
        dag=dag,
    )
    extract_tasks.append(task)

create_datamart_monthly_best_employee = BigQueryExecuteQueryOperator(
    task_id='create_datamart_monthly_best_employee',
    sql="""
    CREATE OR REPLACE TABLE `datamart_monthly_best_employee` AS
    with employee_revenue as (
        select
            e.firstName || ' ' || e.lastName as employee_name,
            date_trunc('month', o.orderDate::timestamp) as month,
            sum((od.unitPrice - (od.unitPrice * od.discount)) * od.quantity) as gross_revenue
        from
            fact_orders o
        join
            fact_order_details od on o.order_id = od.order_id
        join
            dim_employees e on o.employee_id = e.employee_id
        group by
            e.firstName, e.lastName, date_trunc('month', o.orderDate::timestamp)
    )
    select
        employee_name,
        month,
        gross_revenue
    from
        employee_revenue
    order by
        month, gross_revenue desc
    """,
    use_legacy_sql=False,
    dag=dag,
)

create_datamart_monthly_category_sold = BigQueryExecuteQueryOperator(
    task_id='create_datamart_monthly_category_sold',
    sql="""
    CREATE OR REPLACE TABLE `datamart_monthly_category_sold` AS
    with category_sales as (
        select
            c.categoryName,
            date_trunc('month', o.orderDate::timestamp) as month,
            sum(od.quantity) as total_sold
        from
            fact_orders o
        join
            fact_order_details od on o.order_id = od.order_id
        join
            dim_products p on od.product_id = p.product_id
        join
            dim_categories c on p.category_id = c.category_id
        group by
            c.categoryName, date_trunc('month', o.orderDate::timestamp)
    )
    select
        categoryName,
        month,
        total_sold
    from
        category_sales
    order by
        month, total_sold desc
    """,
    use_legacy_sql=False,
    dag=dag,
)

create_datamart_monthly_supplier_gross_revenue = BigQueryExecuteQueryOperator(
    task_id='create_datamart_monthly_supplier_gross_revenue',
    sql="""
    CREATE OR REPLACE TABLE `datamart_monthly_supplier_gross_revenue` AS
    with supplier_revenue as (
        select
            s.companyName,
            date_trunc('month', o.orderDate::timestamp) as month,
            sum((od.unitPrice - (od.unitPrice * od.discount)) * od.quantity) as gross_revenue
        from
            fact_orders o
        join
            fact_order_details od on o.order_id = od.order_id
        join
            dim_products p on od.product_id = p.product_id
        join
            dim_suppliers s on p.supplier_id = s.supplier_id
        group by
            s.companyName, date_trunc('month', o.orderDate::timestamp)
    )
    select
        companyName,
        month,
        gross_revenue
    from
        supplier_revenue
    order by
        companyName, month
    """,
    use_legacy_sql=False,
    dag=dag,
)

save_to_gcs_tasks = []
load_to_bq_tasks = []

for table in table_names:
    save_to_gcs = PostgresToGCSOperator(
        task_id=f'save_{table}_to_gcs',
        postgres_conn_id='air',
        sql=f'SELECT * FROM {table}',
        bucket_name='your-gcs-bucket',
        filename=f'data/{table}.csv',
        export_format='csv',
        field_delimiter=',',
        quote_character='"',
        dag=dag,
    )

    load_to_bq = GCSToBigQueryOperator(
        task_id=f'load_{table}_to_bq',
        bucket='your-gcs-bucket',
        source_objects=[f'data/{table}.csv'],
        destination_project_dataset_table=f'your_project.your_dataset.{table}',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
        dag=dag,
    )

    save_to_gcs_tasks.append(save_to_gcs)
    load_to_bq_tasks.append(load_to_bq)

for extract_task, save_to_gcs_task, load_to_bq_task in zip(extract_tasks, save_to_gcs_tasks, load_to_bq_tasks):
    extract_task >> save_to_gcs_task >> load_to_bq_task

for task in load_to_bq_tasks:
    task >> create_datamart_monthly_best_employee
    task >> create_datamart_monthly_category_sold
    task >> create_datamart_monthly_supplier_gross_revenue
