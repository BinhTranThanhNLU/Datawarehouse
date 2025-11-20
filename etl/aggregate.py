import pandas as pd
from database import warehouse_engine, presentation_engine
from .logger import log_start, log_end


def run_aggregate():
    log_id = log_start("aggregate_presentation")

    try:
        if warehouse_engine is None or presentation_engine is None:
            raise Exception("Database engines not initialized.")

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
            AVG(CASE 
                WHEN c.rain_tomorrow = 'Yes' THEN 1.0
                WHEN c.rain_tomorrow = 'No' THEN 0.0
                ELSE 0.5
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
    except Exception as e:
        error_message = f"Error during aggregation: {e}"
        print(error_message)
        if log_id:
            log_end(log_id, 'failed', 0, error_message)
        raise e


if __name__ == "__main__":
    run_aggregate()
