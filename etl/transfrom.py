import pandas as pd
from database import staging_engine, warehouse_engine
from logger import log_start, log_end


def run_transform():
    # 2. Gọi log_start("transform")
    log_id = log_start("transform")

    # 3. Lưu log_id để kết thúc log sau khi chạy xong
    records_loaded = 0

    try:
        # 4. Kiểm tra kết nối Database
        if staging_engine is None or warehouse_engine is None:
            # Không - Ghi log cảnh báo: "Không thể kết nối database" → Kết thúc tiến trình
            error_message = "Không thể kết nối database"
            print(f"Cảnh báo: {error_message}")
            if log_id:
                log_end(log_id, 'failed', 0, error_message)
            return

        # Có - Tiếp tục
        print("Kết nối database thành công.")

        # 5. Đọc dữ liệu từ /staging_layer/
        # SELECT * FROM staging_layer.weather_raw;
        print("Bước 5: Đọc dữ liệu từ staging_layer/SELECT * FROM staging_layer.weather_raw")
        with staging_engine.connect() as conn:
            df_staging = pd.read_sql_table('weather_raw', conn)

        # 6. Đọc dữ liệu thành công?
        if df_staging.empty:
            # Không - Ghi log success (nhưng 0 records)
            success_message = "Không có dữ liệu trong staging layer để transform"
            print(success_message)
            log_end(log_id, 'success', 0, success_message)
            return

        # Có - Tiếp tục
        print(f"Đọc thành công {len(df_staging)} bản ghi từ staging layer.")

        # 6. Xử lý dữ liệu trước khi tạo Dimensions (xử lý null, loại bỏ record không hợp lệ)
        print("Bước 6: Xử lý dữ liệu - làm sạch null values và loại bỏ records không hợp lệ")

        # Xử lý NULL values cho các cột categorical
        cat_cols = ['WindGustDir', 'WindDir9am', 'WindDir3pm', 'RainToday', 'RainTomorrow']
        df_staging[cat_cols] = df_staging[cat_cols].fillna('Unknown')

        # Loại bỏ records không hợp lệ (missing Date hoặc Location)
        initial_count = len(df_staging)
        df_staging = df_staging.dropna(subset=['Date', 'Location'])
        final_count = len(df_staging)
        print(f"Đã loại bỏ {initial_count - final_count} records không hợp lệ")

        # 7. Tạo và Load Dimension Tables (Dimension: dim_location, Dimension: dim_date, mension: dim_weather_condition)
        print("Bước 7: Tạo và Load Dimension Tables")

        # =======================================
        # 7.1. Create dim_location
        # =======================================
        print("7.1. Tạo dim_location...")
        df_dim_location = (
            pd.DataFrame(df_staging['Location'].unique(), columns=['location_name'])
            .sort_values('location_name')
            .reset_index(drop=True)
        )
        df_dim_location['location_key'] = range(1, len(df_dim_location) + 1)
        df_dim_location.to_sql('dim_location', con=warehouse_engine, if_exists='replace', index=False)
        df_dim_location_with_keys = pd.read_sql_table('dim_location', con=warehouse_engine)
        print(f"Đã tạo dim_location với {len(df_dim_location)} records")

        # =======================================
        # 7.2. Create dim_date
        # =======================================
        print("7.2. Tạo dim_date...")
        df_staging['Date'] = pd.to_datetime(df_staging['Date'])
        df_dim_date = pd.DataFrame(df_staging['Date'].unique(), columns=['full_date'])
        df_dim_date = df_dim_date.dropna()
        df_dim_date['year'] = df_dim_date['full_date'].dt.year
        df_dim_date['month'] = df_dim_date['full_date'].dt.month
        df_dim_date['day'] = df_dim_date['full_date'].dt.day
        df_dim_date['quarter'] = df_dim_date['full_date'].dt.quarter
        df_dim_date['day_of_week'] = df_dim_date['full_date'].dt.day_name()

        df_dim_date = df_dim_date.sort_values('full_date').reset_index(drop=True)
        df_dim_date['date_key'] = range(1, len(df_dim_date) + 1)

        df_dim_date.to_sql('dim_date', con=warehouse_engine, if_exists='replace', index=False)
        df_dim_date_with_keys = pd.read_sql_table('dim_date', con=warehouse_engine)
        print(f"Đã tạo dim_date với {len(df_dim_date)} records")

        # =======================================
        # 7.3. Create dim_weather_condition
        # =======================================
        print("7.3. Tạo dim_weather_condition...")
        condition_cols = ['WindGustDir', 'WindDir9am', 'WindDir3pm', 'RainToday', 'RainTomorrow']

        df_dim_condition = (
            df_staging[condition_cols]
            .drop_duplicates()
            .sort_values(condition_cols)
            .reset_index(drop=True)
        )

        df_dim_condition.columns = [
            'wind_gust_dir', 'wind_dir9am', 'wind_dir3pm',
            'rain_today', 'rain_tomorrow'
        ]
        df_dim_condition['condition_key'] = range(1, len(df_dim_condition) + 1)

        df_dim_condition.to_sql(
            'dim_weather_condition',
            con=warehouse_engine, if_exists='replace', index=False
        )
        df_dim_condition_with_keys = pd.read_sql_table('dim_weather_condition', con=warehouse_engine)
        print(f"Đã tạo dim_weather_condition với {len(df_dim_condition)} records")

        # =======================================
        # 8. Tập tục load Dimension
        # =======================================
        print("Bước 8: Hoàn tất load các Dimension tables")

        # =======================================
        # 10. Tạo dữ liệu Fact (Merge dimensions)
        # =======================================
        print("Bước 10: Tạo dữ liệu Fact (Merge dimensions)")
        df_fact_data = df_staging.copy()

        # Merge Location
        df_fact_data = pd.merge(
            df_fact_data,
            df_dim_location_with_keys,
            left_on='Location',
            right_on='location_name',
            how='left'
        )

        # Merge Date
        df_fact_data = pd.merge(
            df_fact_data,
            df_dim_date_with_keys,
            left_on='Date',
            right_on='full_date',
            how='left'
        )

        # Merge Condition
        df_fact_data = pd.merge(
            df_fact_data,
            df_dim_condition_with_keys,
            left_on=['WindGustDir', 'WindDir9am', 'WindDir3pm', 'RainToday', 'RainTomorrow'],
            right_on=['wind_gust_dir', 'wind_dir9am', 'wind_dir3pm', 'rain_today', 'rain_tomorrow'],
            how='left'
        )

        # Remove extra merge columns
        drop_cols = [
            'location_name', 'full_date',
            'wind_gust_dir', 'wind_dir9am', 'wind_dir3pm',
            'rain_today', 'rain_tomorrow'
        ]
        df_fact_data = df_fact_data.drop(columns=[c for c in drop_cols if c in df_fact_data])

        # =======================================
        # Select columns for fact table
        # =======================================
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

        # Remove rows with missing foreign keys
        df_fact_weather = df_fact_weather.dropna(subset=['location_key', 'date_key', 'condition_key'])

        # Remove duplicates
        df_fact_weather = df_fact_weather.drop_duplicates()

        # Kiểm tra số lượng records sau khi merge
        records_loaded = len(df_fact_weather)

        # Kiểm tra đáng số bản ghi fact_weather > 0 không?
        if records_loaded == 0:
            # Nếu = 0: Ghi log lỗi
            error_message = "Không có dữ liệu hợp lệ sau khi merge dimensions"
            print(f"Lỗi: {error_message}")
            log_end(log_id, 'failed', 0, error_message)
            return

        # Nếu > 0: Tiếp tục
        print(f"Có {records_loaded} records hợp lệ để load vào fact table")

        # =======================================
        # 11. Tiến tục load
        # =======================================
        print("Bước 11: Load dữ liệu vào fact_weather...")
        df_fact_weather.to_sql(
            'fact_weather',
            con=warehouse_engine,
            if_exists='replace',
            index=False,
            chunksize=1000
        )

        # 12. Ghi log: log_end(log_id, "success", records, "Data transformed to warehouse")
        success_message = "Data transformed to warehouse"
        print(f"Bước 12: Ghi log thành công - {records_loaded} records đã được load")
        log_end(log_id, 'success', records_loaded, success_message)

    except Exception as e:
        # Xử lý lỗi: Ghi log lỗi và kết thúc
        error_message = f"Error during transformation: {e}"
        print(f"Lỗi xảy ra: {error_message}")
        if log_id:
            log_end(log_id, 'failed', 0, error_message)
        raise e


if __name__ == "__main__":
    # 1. runTransformLoadForConfig (entry point)
    print("Bước 1: Bắt đầu quá trình Transform")
    run_transform()