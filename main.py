# # weather_dwh/control/main.py
from etl import extract, transform, aggregate
import time
from database import staging_engine, warehouse_engine, presentation_engine, log_engine

def main_pipeline():
    start_time = time.time()
    print("--- [START] ETL Pipeline ---")
    
    # Kiểm tra tất cả các kết nối DB trước khi chạy
    if not all([staging_engine, warehouse_engine, presentation_engine, log_engine]):
        print("--- [FATAL] Database connection failed. Aborting pipeline. ---")
        print("--- Please check your credentials in 'src/database.py'. ---")
        return

    # --- 1. EXTRACT ---
    print("\n--- Running EXTRACTION (E) step... ---")
    try:
        extract.run_extract()
        print("--- [SUCCESS] Extraction complete. ---")
    except Exception as e:
        print(f"--- [FAILED] Extraction failed: {e} ---")
        print("--- Aborting pipeline. ---")
        return # Dừng pipeline nếu extract lỗi

    # --- 2. TRANSFORM ---
    print("\n--- Running TRANSFORM (T) step... ---")
    try:
        transform.run_transform()
        print("--- [SUCCESS] Transformation complete. ---")
    except Exception as e:
        print(f"--- [FAILED] Transformation failed: {e} ---")
        print("--- Aborting pipeline. ---")
        return # Dừng pipeline nếu transform lỗi

    # --- 3. AGGREGATE (Load to Presentation) ---
    print("\n--- Running AGGREGATION (L) step... ---")
    try:
        aggregate.run_aggregate()
        print("--- [SUCCESS] Aggregation complete. ---")
    except Exception as e:
        print(f"--- [FAILED] Aggregation failed: {e} ---")
        return

    end_time = time.time()
    print(f"\n--- [COMPLETE] ETL Pipeline finished in {end_time - start_time:.2f} seconds. ---")

if __name__ == "__main__":
    main_pipeline()