# weather_dwh/control/database.py
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

DB_USER = 'root'
DB_PASSWORD = '' # <--- THAY MẬT KHẨU CỦA BẠN VÀO ĐÂY
DB_HOST = 'localhost'
DB_PORT = '3306'

# Tên các database
STAGING_DB = 'staging_layer'
WAREHOUSE_DB = 'warehouse_layer'
PRESENTATION_DB = 'presentation_layer'
LOG_DB = 'log'

def get_engine(db_name):
    """Tạo một SQLAlchemy engine cho một database cụ thể."""
    try:
        conn_string = f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{db_name}"
        engine = create_engine(conn_string, pool_pre_ping=True)
        # Kiểm tra kết nối
        with engine.connect():
            pass 
        print(f"Successfully connected to database: {db_name}")
        return engine
    except SQLAlchemyError as e:
        print(f"Error connecting to {db_name} at {DB_HOST}: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred while connecting to {db_name}: {e}")
        return None

# Tạo các engine cho từng layer
print("Initializing database connections...")
staging_engine = get_engine(STAGING_DB)
warehouse_engine = get_engine(WAREHOUSE_DB)
presentation_engine = get_engine(PRESENTATION_DB)
log_engine = get_engine(LOG_DB)
print("Database connections initialized.")