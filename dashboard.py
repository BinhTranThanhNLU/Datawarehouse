import streamlit as st
import pandas as pd
import sys
import os

# --- 1. SETUP KẾT NỐI ---
# Trỏ đường dẫn để import được file database.py trong thư mục control
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'control')))
try:
    from database import get_engine, PRESENTATION_DB
except ImportError:
    # Phòng trường hợp chạy từ thư mục gốc
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../control')))
    from database import get_engine, PRESENTATION_DB

# Cấu hình trang
st.set_page_config(page_title="Weather Dashboard Group 2", layout="wide")
st.title("⛈️ Báo Cáo Thời Tiết (Data Warehouse)")

# --- 2. LẤY DỮ LIỆU ---
engine = get_engine(PRESENTATION_DB)

if engine:
    # Lấy toàn bộ dữ liệu bảng summary
    # Lưu ý: Cần join thêm bảng dim_location (từ warehouse) nếu muốn hiện tên địa điểm thay vì số
    # Nhưng để đơn giản, ta cứ dùng location_key có sẵn trong bảng presentation
    query = "SELECT * FROM weather_summary_daily"
    df = pd.read_sql(query, con=engine)

    if not df.empty:
        # --- 3. TẠO BỘ LỌC (SIDEBAR) ---
        st.sidebar.header("Bộ lọc dữ liệu")
        
        # Lấy danh sách các location_key duy nhất
        unique_locations = sorted(df['location_key'].unique())
        
        # Tạo hộp chọn
        selected_location = st.sidebar.selectbox(
            "Chọn địa điểm (Location Key):", 
            unique_locations
        )

        # Lọc dataframe theo địa điểm đã chọn
        df_filtered = df[df['location_key'] == selected_location]
        
        # Sắp xếp lại theo thời gian (date_key tăng dần) để vẽ biểu đồ cho đúng chiều
        df_filtered = df_filtered.sort_values(by='date_key', ascending=True)

        # --- 4. HIỂN THỊ METRICS (Dòng mới nhất của địa điểm đó) ---
        if not df_filtered.empty:
            latest = df_filtered.iloc[-1] # Lấy dòng cuối cùng (ngày mới nhất)
            
            st.subheader(f"📍 Thông tin Location: {selected_location} | Ngày (Key): {latest['date_key']}")
            
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Nhiệt độ TB", f"{latest['avg_temp']:.1f} °C")
            col2.metric("Nhiệt độ Max", f"{latest['max_temp']:.1f} °C")
            col3.metric("Lượng mưa", f"{latest['total_rainfall']:.1f} mm")
            col4.metric("Khả năng mưa", f"{latest['rain_probability'] * 100:.0f} %")

            st.divider()

            # --- 5. VẼ BIỂU ĐỒ ---
            st.subheader("📈 Xu hướng nhiệt độ theo thời gian")
            
            # Chỉ lấy các cột cần vẽ và set index là date_key
            chart_data = df_filtered.set_index('date_key')[['min_temp', 'avg_temp', 'max_temp']]
            
            # Vẽ biểu đồ line
            st.line_chart(chart_data)
            
            # --- 6. XEM DỮ LIỆU CHI TIẾT ---
            with st.expander("Xem dữ liệu dạng bảng"):
                st.dataframe(df_filtered)
        else:
            st.warning("Không có dữ liệu cho địa điểm này.")
    else:
        st.error("Bảng weather_summary_daily đang trống. Hãy kiểm tra lại quy trình ETL (Aggregate).")
else:
    st.error("Không thể kết nối Database Presentation.")