from flask import Flask, jsonify, render_template_string
import pandas as pd
import sys
import os

# Thêm đường dẫn để import được module database từ thư mục control
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'control')))

# Tận dụng code kết nối DB có sẵn của nhóm
from database import get_engine, PRESENTATION_DB

app = Flask(__name__)

# Kết nối đến Presentation Layer
# Theo file database.py, biến này tên là PRESENTATION_DB = 'presentation_layer'
db_engine = get_engine(PRESENTATION_DB)

@app.route('/')
def home():
    return "<h1>Weather Data Warehouse API</h1><p>Truy cập /api/daily-summary để lấy dữ liệu JSON.</p>"

@app.route('/api/daily-summary')
def get_daily_summary():
    try:
        if db_engine is None:
            return jsonify({"error": "Không thể kết nối database"}), 500
            
        # Query bảng weather_summary_daily trong Presentation Layer
        # Các cột: avg_temp, max_temp, min_temp, rain_probability...
        query = "SELECT * FROM weather_summary_daily ORDER BY date_key DESC LIMIT 10"
        
        # Dùng pandas đọc cho nhanh, giống cách làm trong file aggregate.py
        df = pd.read_sql(query, con=db_engine)
        
        # Chuyển đổi dữ liệu thành JSON
        result = df.to_dict(orient='records')
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/dashboard')
def dashboard():
    # Ví dụ hiển thị dạng bảng HTML đơn giản
    if db_engine is None:
        return "Lỗi kết nối DB"
    
    df = pd.read_sql("SELECT * FROM weather_summary_daily LIMIT 20", con=db_engine)
    
    # Render bảng HTML từ dataframe
    return render_template_string("""
        <style>
            table {border-collapse: collapse; width: 100%;}
            th, td {border: 1px solid black; padding: 8px; text-align: left;}
            th {background-color: #f2f2f2;}
        </style>
        <h2>Báo cáo thời tiết hàng ngày (Từ DWH Presentation Layer)</h2>
        {{ table|safe }}
    """, table=df.to_html(classes='data', header="true", index=False))

if __name__ == '__main__':
    app.run(debug=True, port=5000)