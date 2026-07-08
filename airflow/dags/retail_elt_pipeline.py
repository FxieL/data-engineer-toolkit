from airflow.decorators import dag, task
from datetime import datetime, timedelta
import pandas as pd
import os
from sqlalchemy import create_engine

# Path raw data sesuai YAML bawaan aslinya
RAW_BASE_PATH = "/opt/airflow/dags/data/raw/fxiel_retail"
# URL Koneksi Neon kamu
NEON_URL = "postgresql://neondb_owner:npg_r4aTOvEI6ZBq@ep-morning-violet-aobaczkk-pooler.c-2.ap-southeast-1.aws.neon.tech/neondb?sslmode=require"

default_args = {
    'owner': 'fxiel',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='retail_elt_neon_pipeline',
    default_args=default_args,
    start_date=datetime(2026, 7, 8),
    schedule='@daily',
    catchup=False
)
def retail_elt_neon_pipeline():

    @task
    def load_to_neon():
        # Bikin 'mesin' buat konek ke Neon
        engine = create_engine(NEON_URL)
        
        # Folder data yang mau di-load ke database
        folders = ['orders', 'products', 'customers'] 
        
        for folder in folders:
            raw_path = os.path.join(RAW_BASE_PATH, folder)
            
            # Cek apakah foldernya ada
            if os.path.exists(raw_path):
                files = [f for f in os.listdir(raw_path) if f.endswith('.csv')]
                
                for file in files:
                    # 1. EXTRACT: Baca CSV mentah
                    file_path = os.path.join(raw_path, file)
                    df = pd.read_csv(file_path)
                    
                    # 2. TRANSFORM: Bersihkan data (hapus yang kosong)
                    df = df.dropna()
                    
                    # 3. LOAD: Tembak langsung ke Neon!
                    # Kita pakai nama folder (orders, products, dll) sebagai nama tabel
                    df.to_sql(
                        name=folder, 
                        con=engine, 
                        if_exists='replace', # Kalau tabelnya udah ada, akan diganti dengan data baru
                        index=False
                    )
                    print(f"✅ Berhasil memproses dan mengirim data {folder} ke Neon!")

    load_to_neon()

retail_elt_neon_pipeline()