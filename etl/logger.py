# weather_dwh/control/etl/logger.py
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
from database import log_engine  # đảm bảo file database.py có log_engine


def log_start(process_name: str):
    """
    Ghi log bắt đầu chạy 1 tiến trình ETL.
    Trả về log_id để dùng tiếp cho log_end.
    """
    if log_engine is None:
        print(f"[LOG] log_engine not initialized. Skip start log for {process_name}")
        return None

    try:
        with log_engine.begin() as conn:
            result = conn.execute(
                text("""
                    INSERT INTO etl_log (process_name, status, start_time)
                    VALUES (:process_name, 'running', :start_time)
                """),
                {
                    "process_name": process_name,
                    "start_time": datetime.now()
                }
            )
            return result.lastrowid  # MySQL OK
    except SQLAlchemyError as e:
        print(f"[LOG ERROR] Failed to start log for {process_name}: {e}")
        return None


def log_end(log_id: int, status: str, records_loaded: int = 0, message: str = ""):
    """
    Cập nhật log khi tiến trình ETL kết thúc.
    """
    if log_engine is None or log_id is None:
        print("[LOG] Skip end log. No log_id or engine not initialized.")
        return

    try:
        with log_engine.begin() as conn:
            conn.execute(
                text("""
                    UPDATE etl_log
                    SET status = :status,
                        records_loaded = :records,
                        message = :message,
                        end_time = :end_time
                    WHERE id = :log_id
                """),
                {
                    "status": status,
                    "records": records_loaded,
                    "message": message,
                    "end_time": datetime.now(),
                    "log_id": log_id
                }
            )
    except SQLAlchemyError as e:
        print(f"[LOG ERROR] Failed to end log for id {log_id}: {e}")
