# weather_dwh/control/etl/aggregate.py
import pandas as pd
from database import warehouse_engine, presentation_engine
from .logger import log_start, log_end


def run_aggregate():
    log_id = log_start("aggregate_presentation")
    records_loaded = 0

    try:
        if warehouse_engine is None or presentation_engine is None:
            raise Exception("Database engines not initialized.")

        print("Running aggregation query on warehouse...")

        # Câu SQL này định nghĩa logic nghiệp vụ cho bảng summary
        # Nó đọc từ warehouse và tính toán các chỉ số
        query = """
        SELECT
            f.location_key,
            f.date_key,
            AVG((f.min_temp + f.max_temp) / 2) AS avg_temp,
            MAX(f.max_temp) AS max_temp,
            MIN(f.min_temp) AS min_temp,
            AVG((f.humidity9am + f.humidity3pm) / 2) AS avg_humidity,
            SUM(f.rainfall) AS total_rainfall,
            c.wind_gust_dir AS dominant_wind_dir,

            -- Quy tắc nghiệp vụ cho 'rain_probability':
            -- 1.0 nếu 'Yes', 0.0 nếu 'No', 0.5 nếu 'Unknown'
            AVG(CASE 
                WHEN c.rain_tomorrow = 'Yes' THEN 1.0
                WHEN c.rain_tomorrow = 'No' THEN 0.0
                ELSE 0.5 -- Xử lý trường hợp 'Unknown'
            END) AS rain_probability

        FROM
            warehouse_layer.fact_weather AS f
        JOIN
            warehouse_layer.dim_weather_condition AS c ON f.condition_key = c.condition_key
        JOIN
            warehouse_layer.dim_date AS d ON f.date_key = d.date_key
        GROUP BY
            f.location_key,
            f.date_key,
            c.wind_gust_dir
        """

        # Đọc dữ liệu đã tổng hợp từ warehouse
        df_summary = pd.read_sql(query, con=warehouse_engine)

        if df_summary.empty:
            print("No data to aggregate.")
            log_end(log_id, 'success', 0, "No data found in warehouse to aggregate.")
            return

        print(f"Aggregated {len(df_summary)} summary records.")

        # Load dữ liệu summary vào bảng presentation
        print("Loading data into presentation.weather_summary_daily...")
        # [SỬA] Thêm chunksize=1000 để cắt nhỏ dữ liệu khi ghi
        df_summary.to_sql('weather_summary_daily', con=presentation_engine, if_exists='replace', index=False,
                          chunksize=1000)

        records_loaded = len(df_summary)
        print(f"Successfully loaded {records_loaded} records to presentation layer.")
        log_end(log_id, 'success', records_loaded, "Data aggregated to presentation layer.")

    except Exception as e:
        error_message = f"Error during aggregation: {e}"
        print(error_message)
        if log_id:
            log_end(log_id, 'failed', 0, error_message)
        # raise e


if __name__ == "__main__":
    run_aggregate()