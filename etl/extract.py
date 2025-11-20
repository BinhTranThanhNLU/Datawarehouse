# weather_dwh/control/etl/extract.py
import pandas as pd
from database import staging_engine
from .logger import log_start, log_end
import os

def run_extract():
    log_id = log_start("extract")
    records_loaded = 0
    
    try:
        if staging_engine is None:
            raise Exception("Staging database engine is not initialized.")
            
        # Xây dựng đường dẫn file CSV (file này nằm trong 'staging')
        # ../../staging/weatherAUS.csv
        script_dir = os.path.dirname(__file__)
        file_path = os.path.join(script_dir, '../../staging/weatherAUS.csv')
        
        print(f"Reading data from {file_path}...")
        # Đọc CSV, coi 'NA' là giá trị NULL
        df = pd.read_csv(file_path, na_values='NA')
        
        print(f"Loaded {len(df)} rows from CSV.")
        print("Loading data into staging_layer.weather_raw...")
        
        # Load vào DB. 'replace' sẽ xóa bảng cũ và tạo lại
        # Rất hữu ích cho staging layer để đảm bảo dữ liệu luôn mới
        df.to_sql('weather_raw', con=staging_engine, if_exists='replace', index=False, chunksize=1000)
        
        records_loaded = len(df)
        print(f"Successfully loaded {records_loaded} records to staging.")
        log_end(log_id, 'success', records_loaded, "Data extracted to staging layer.")
        
    except Exception as e:
        error_message = f"Error during extraction: {e}"
        print(error_message)
        if log_id:
            log_end(log_id, 'failed', 0, error_message)

if __name__ == "__main__":
    run_extract()