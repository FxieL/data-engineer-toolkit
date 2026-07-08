from airflow.decorators import dag, task
from datetime import datetime, timedelta
import pandas as pd
import os
from sqlalchemy import create_engine, text

RAW_BASE_PATH = "/opt/airflow/dags/data/raw/fxiel_retail"
NEON_URL = "postgresql://neondb_owner:npg_r4aTOvEI6ZBq@ep-morning-violet-aobaczkk-pooler.c-2.ap-southeast-1.aws.neon.tech/neondb?sslmode=require"

default_args = {
    'owner': 'fxiel',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='retail_elt_neon_pipeline_final',
    default_args=default_args,
    start_date=datetime(2026, 7, 8),
    schedule='@daily',
    catchup=False
)
def retail_elt_neon_pipeline():

    @task
    def load_to_neon_incremental():
        engine = create_engine(NEON_URL)
        folders = ['orders', 'products', 'customers'] 
        
        for folder in folders:
            raw_path = os.path.join(RAW_BASE_PATH, folder)
            if os.path.exists(raw_path):
                files = [f for f in os.listdir(raw_path) if f.endswith('.csv')]
                for file in files:
                    df = pd.read_csv(os.path.join(raw_path, file))
                    df = df.dropna()
                    
                    # UBAH DI SINI: Gunakan 'append' agar datanya nambah (Incremental), bukan ditimpa
                    df.to_sql(
                        name=folder, 
                        con=engine, 
                        if_exists='append', 
                        index=False
                    )
                    print(f"✅ Berhasil load incremental data {folder}")

    @task
    def create_gold_datamart():
        engine = create_engine(NEON_URL)
        with engine.begin() as conn:
            # Tetap pakai DROP IF EXISTS untuk Datamart, karena Datamart adalah hasil rekapitulasi ulang
            conn.execute(text("DROP TABLE IF EXISTS gold_datamart_orders;"))
            
            sql_query = """
            CREATE TABLE gold_datamart_orders AS
            SELECT 
                o.order_id,
                o.order_date,
                o.payment_method,
                c.customer_id
            FROM orders o
            LEFT JOIN customers c ON o.customer_id = c.customer_id;
            """
            conn.execute(text(sql_query))
            print("✅ Datamart berhasil di-refresh!")

    @task
    def generate_analysis():
        engine = create_engine(NEON_URL)
        
        # Contoh Analisis: Menghitung total transaksi berdasarkan metode pembayaran
        query = """
        SELECT payment_method, COUNT(order_id) as total_transaksi
        FROM gold_datamart_orders
        GROUP BY payment_method
        ORDER BY total_transaksi DESC;
        """
        
        # Baca hasil query langsung pakai Pandas
        df_analysis = pd.read_sql(query, engine)
        
        print("========== HASIL ANALISIS ==========")
        print(df_analysis.to_string(index=False))
        print("====================================")
        
        # Ekspor ke CSV sebagai barang bukti buat di PPT
        os.makedirs("/opt/airflow/dags/data/analysis", exist_ok=True)
        df_analysis.to_csv("/opt/airflow/dags/data/analysis/payment_method_analysis.csv", index=False)

    # Atur urutan jalannya (3 kotak sekarang)
    load_to_neon_incremental() >> create_gold_datamart() >> generate_analysis()

retail_elt_neon_pipeline()