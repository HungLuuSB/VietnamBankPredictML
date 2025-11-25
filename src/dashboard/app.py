import streamlit as st
import pandas as pd
import requests
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time

API_URL = "http://127.0.0.1:8000"
st.set_page_config(page_title="Stock AI Platform", layout="wide", page_icon="📈")

st.markdown("""
    <style>
    .stApp { background-color: #0E1117; }
    .block-container { padding-top: 2rem; }
    </style>
""", unsafe_allow_html=True)


@st.cache_data(ttl=60)
def fetch_supported_stocks():
    try:
        res = requests.get(f"{API_URL}/supported-stocks")
        return res.json().get("stocks", [])
    except:
        return []

@st.cache_data(ttl=30)
def fetch_ranking():
    """Lấy bảng xếp hạng TSSL"""
    try:
        res = requests.get(f"{API_URL}/rankings", params={"type": "tssl_1d"})
        if res.status_code == 200:
            return pd.DataFrame(res.json())
        return pd.DataFrame()
    except:
        return pd.DataFrame()

def fetch_stock_data(symbol, rng):
    hist_df = pd.DataFrame()
    pred_data = None
    try:
        h_res = requests.get(f"{API_URL}/history/{symbol}", params={"range": rng}, timeout=2)
        if h_res.status_code == 200:
            hist_df = pd.DataFrame(h_res.json())
            if not hist_df.empty: hist_df['date'] = pd.to_datetime(hist_df['date'])
        p_res = requests.post(f"{API_URL}/predict", json={"stock_code": symbol}, timeout=2)
        if p_res.status_code == 200:
            pred_data = p_res.json()
    except Exception as e:
        st.error(f"Lỗi kết nối: {e}")
    return hist_df, pred_data

with st.sidebar:
    st.title("🎛️ Control Panel")
    all_stocks = fetch_supported_stocks()
    if not all_stocks:
        st.error("Không kết nối được API!")
        all_stocks = ["VCB"]

    selected_stock = st.selectbox("Chọn Mã Cổ Phiếu", all_stocks)
    time_range = st.select_slider(
        "Khung thời gian", 
        options=["7D", "1M", "3M", "6M", "1Y", "ALL"], 
        value="3M"
    )
    
    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()

tab1, tab2, tab3 = st.tabs(["📊 Dashboard & Dự báo", "🏆 Xếp hạng Thị trường", "🔍 Tra cứu Mã"])
with tab1:
    st.header(f"Biểu đồ kỹ thuật: {selected_stock}")
    df, pred_data = fetch_stock_data(selected_stock, time_range)
    if df.empty:
        st.warning("Chưa có dữ liệu hiển thị.")
    else:
        latest = df.iloc[-1]
        prev = df.iloc[-2] if len(df) > 1 else latest
        price = latest['close']
        change = price - prev['close']
        pct = (change / prev['close']) * 100
        c1, c2, c3 = st.columns(3)
        c1.metric("Giá hiện tại", f"{price:,.0f} ₫", f"{change:,.0f} ({pct:.2f}%)")
        c2.metric("Khối lượng", f"{latest['volume']:,.0f}")
        if pred_data and pred_data.get('predicted_price'):
            p_price = pred_data['predicted_price']
            p_delta = p_price - price
            p_pct = (p_delta / price) * 100
            c3.metric("AI Dự báo (T+1)", f"{p_price:,.0f} ₫", f"{p_delta:,.0f} ({p_pct:.2f}%)", delta_color="inverse")
        else:
            c3.metric("AI Dự báo", "Đang tính toán...", delta_color="off")

        fig = go.Figure()
        if time_range in ["7D", "1M"]:
            fig.add_trace(go.Candlestick(
                x=df['date'], open=df['open'], high=df['high'],
                low=df['low'], close=df['close'], name='OHLC'
            ))
        else:
            fig.add_trace(go.Scatter(
                x=df['date'], y=df['close'], mode='lines', 
                name='Close Price', line=dict(color='#00C9FF', width=2)
            ))
        if pred_data and pred_data.get('predicted_price'):
            last_d = df.iloc[-1]['date']
            next_d = last_d + timedelta(days=1)
            if next_d.weekday() >= 5: next_d += timedelta(days=2)
            fig.add_trace(go.Scatter(
                x=[last_d, next_d], y=[price, pred_data['predicted_price']],
                mode='lines+markers', name='AI Forecast',
                line=dict(color='#FF4B4B', width=2, dash='dot'),
                marker=dict(size=8, symbol='star')
            ))

        fig.update_layout(template="plotly_dark", height=500, margin=dict(l=0,r=0,t=30,b=0))
        st.plotly_chart(fig, use_container_width=True)

with tab2:
    st.header("🏆 Top Tăng trưởng (TSSL 1 Ngày)")
    df_rank = fetch_ranking()
    if not df_rank.empty:
        df_rank['Tăng trưởng (%)'] = (df_rank['metric_value'] * 100).round(2)
        display_df = df_rank[['symbol', 'Tăng trưởng (%)', 'date']]
        display_df.columns = ['Mã CK', 'Tăng trưởng (%)', 'Ngày cập nhật']
        st.dataframe(
            display_df,
            column_config={
                "Tăng trưởng (%)": st.column_config.NumberColumn(
                    "Tăng trưởng (%)",
                    format="%.2f %%",
                ),
            },
            use_container_width=True,
            hide_index=True
        )
        top5 = df_rank.head(5).sort_values('metric_value', ascending=True)
        fig_bar = go.Figure(go.Bar(
            x=top5['metric_value'],
            y=top5['symbol'],
            orientation='h',
            marker=dict(color=top5['metric_value'], colorscale='Viridis')
        ))
        fig_bar.update_layout(title="Top 5 Mã Dẫn Đầu", template="plotly_dark", height=300)
        st.plotly_chart(fig_bar, use_container_width=True)
    else:
        st.info("Chưa có dữ liệu xếp hạng. Vui lòng chạy Job 4.")

with tab3:
    st.header("🔍 Tra cứu Mã Chứng Khoán")
    search_term = st.text_input("Nhập từ khóa", "").upper()
    if search_term:
        filtered_stocks = [s for s in all_stocks if search_term in s]
        if filtered_stocks:
            st.success(f"Tìm thấy {len(filtered_stocks)} mã khớp:")
            cols = st.columns(5)
            for i, stock in enumerate(filtered_stocks):
                with cols[i % 5]:
                    if st.button(stock, key=f"btn_{stock}"):
                        st.toast(f"Đã copy mã {stock}. Hãy chọn ở Sidebar để xem!")
        else:
            st.warning("Không tìm thấy mã nào khớp.")
