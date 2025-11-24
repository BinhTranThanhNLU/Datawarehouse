# weather_dwh/control/etl/transform.py
import pandas as pd
from database import staging_engine, warehouse_engine
from .logger import log_start, log_end


def run_transform():
    log_id = log_start("transform")
    records_loaded = 0

    try:
        if staging_engine is None or warehouse_engine is None:
            raise Exception("Database engines not initialized.")

        # 1. Đọc dữ liệu từ Staging
        print("Reading data from staging_layer.weather_raw...")
        with staging_engine.connect() as conn:
            df_staging = pd.read_sql_table('weather_raw', conn)

        if df_staging.empty:
            print("No data in staging layer to transform.")
            log_end(log_id, 'success', 0, "No data in staging.")
            return

        # 2. Xử lý NULL (NaN) trước khi tạo dimensions
        # Thay thế NaN trong các cột categorical bằng 'Unknown'
        cat_cols = ['WindGustDir', 'WindDir9am', 'WindDir3pm', 'RainToday', 'RainTomorrow']
        df_staging[cat_cols] = df_staging[cat_cols].fillna('Unknown')

        # Xóa các dòng có Date hoặc Location bị null
        df_staging = df_staging.dropna(subset=['Date', 'Location'])

        # --- 3. Tạo và Load dim_location ---
        print("Processing dim_location...")
        df_dim_location = pd.DataFrame(df_staging['Location'].unique(), columns=['location_name'])
        # [SỬA] Tạo location_key (1, 2, 3...)
        df_dim_location['location_key'] = range(1, len(df_dim_location) + 1)
        df_dim_location.to_sql('dim_location', con=warehouse_engine, if_exists='replace', index=False)
        # Đọc lại (lúc này đã chắc chắn có location_key)
        df_dim_location_with_keys = pd.read_sql_table('dim_location', con=warehouse_engine)

        # --- 4. Tạo và Load dim_date ---
        print("Processing dim_date...")
        df_staging['Date'] = pd.to_datetime(df_staging['Date'])
        df_dim_date = pd.DataFrame(df_staging['Date'].unique(), columns=['full_date'])
        df_dim_date = df_dim_date.dropna()
        df_dim_date['year'] = df_dim_date['full_date'].dt.year
        df_dim_date['month'] = df_dim_date['full_date'].dt.month
        df_dim_date['day'] = df_dim_date['full_date'].dt.day
        df_dim_date['quarter'] = df_dim_date['full_date'].dt.quarter
        df_dim_date['day_of_week'] = df_dim_date['full_date'].dt.day_name()
        # [SỬA] Tạo date_key
        df_dim_date['date_key'] = range(1, len(df_dim_date) + 1)
        df_dim_date.to_sql('dim_date', con=warehouse_engine, if_exists='replace', index=False)
        df_dim_date_with_keys = pd.read_sql_table('dim_date', con=warehouse_engine)

        # --- 5. Tạo và Load dim_condition ---
        print("Processing dim_weather_condition...")
        condition_cols = ['WindGustDir', 'WindDir9am', 'WindDir3pm', 'RainToday', 'RainTomorrow']
        df_dim_condition = df_staging[condition_cols].drop_duplicates().reset_index(drop=True)
        # Đổi tên cột để khớp với schema
        df_dim_condition.columns = ['wind_gust_dir', 'wind_dir9am', 'wind_dir3pm', 'rain_today', 'rain_tomorrow']
        # [SỬA] Tạo condition_key
        df_dim_condition['condition_key'] = range(1, len(df_dim_condition) + 1)
        df_dim_condition.to_sql('dim_weather_condition', con=warehouse_engine, if_exists='replace', index=False)
        df_dim_condition_with_keys = pd.read_sql_table('dim_weather_condition', con=warehouse_engine)

        # --- 6. Chuẩn bị dữ liệu cho Fact Table (Merge để lấy Keys) ---
        print("Merging dimensions to create fact table data...")
        df_fact_data = df_staging

        # Merge Location Key
        df_fact_data = pd.merge(df_fact_data, df_dim_location_with_keys, left_on='Location', right_on='location_name',
                                how='left')

        # Merge Date Key
        df_fact_data = pd.merge(df_fact_data, df_dim_date_with_keys, left_on='Date', right_on='full_date', how='left')

        # Merge Condition Key
        df_fact_data = pd.merge(df_fact_data, df_dim_condition_with_keys,
                                left_on=['WindGustDir', 'WindDir9am', 'WindDir3pm', 'RainToday', 'RainTomorrow'],
                                right_on=['wind_gust_dir', 'wind_dir9am', 'wind_dir3pm', 'rain_today', 'rain_tomorrow'],
                                how='left')

        # --- 7. Chọn lọc và đổi tên cột cho Bảng Fact ---
        fact_columns_map = {
            'location_key': 'location_key',
            'date_key': 'date_key',
            'condition_key': 'condition_key',
            'MinTemp': 'min_temp',
            'MaxTemp': 'max_temp',
            'Rainfall': 'rainfall',
            'Evaporation': 'evaporation',
            'Sunshine': 'sunshine',
            'Humidity9am': 'humidity9am',
            'Humidity3pm': 'humidity3pm',
            'Pressure9am': 'pressure9am',
            'Pressure3pm': 'pressure3pm',
            'WindGustSpeed': 'windgustspeed',
            'WindSpeed9am': 'windspeed9am',
            'WindSpeed3pm': 'windspeed3pm',
            'Cloud9am': 'cloud9am',
            'Cloud3pm': 'cloud3pm',
            'Temp9am': 'temp9am',
            'Temp3pm': 'temp3pm'
        }

        df_fact_weather = df_fact_data[list(fact_columns_map.keys())].rename(columns=fact_columns_map)

        # Xóa các dòng có foreign key bị null (do merge thất bại)
        df_fact_weather = df_fact_weather.dropna(subset=['location_key', 'date_key', 'condition_key'])

        # --- 8. Load Fact Table ---
        print("Loading fact_weather...")
        df_fact_weather.to_sql('fact_weather', con=warehouse_engine, if_exists='replace', index=False, chunksize=1000)

        records_loaded = len(df_fact_weather)
        print(f"Successfully loaded {records_loaded} records to fact_weather.")
        log_end(log_id, 'success', records_loaded, "Data transformed to warehouse.")

    except Exception as e:
        error_message = f"Error during transformation: {e}"
        print(error_message)
        if log_id:
            log_end(log_id, 'failed', 0, error_message)
        raise e  # <--- THÊM DÒNG NÀY (Để báo cho main.py biết là có lỗi)


if __name__ == "__main__":
    run_transform()