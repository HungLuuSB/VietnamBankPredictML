import sys
import json
import pandas as pd
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from loguru import logger
from datetime import datetime
from dateutil.relativedelta import relativedelta

file_path = Path(__file__).resolve()
project_root = file_path.parent.parent.parent
sys.path.append(str(project_root))

from src.utils.config_loader import config
from src.data_ingestion import data_helpers
from .schemas import StockDataPoint, SearchResult, PredictionMessage
from aiokafka import AIOKafkaConsumer

app = FastAPI(
    title="VietnamStock",
    description="Stock view and prediction."
)

def _get_start_date_from_range(range_str: str) -> str | None:
    today = datetime.now()
    range_str = range_str.upper()
    
    if range_str == "7D":
        return (today - relativedelta(days=7)).isoformat()
    if range_str == "1M":
        return (today - relativedelta(months=1)).isoformat()
    if range_str == "1Q": # 1 Quý
        return (today - relativedelta(months=3)).isoformat()
    if range_str == "1Y":
        return (today - relativedelta(years=1)).isoformat()
    if range_str == "ALL":
        return None
    return None

@app.get("/api/v1/history/{symbol}", 
         response_model=list[StockDataPoint],
         tags=["Stock Data"])
async def get_price_history(
    symbol: str,
    range: str = Query("1Y", enum=["7D", "1M", "1Q", "1Y", "ALL"])
):
    logger.info(f"[api/get_price_history] Getting price history for {symbol} (range: {range})")
    _start_date = _get_start_date_from_range(range)
    df_history: pd.DataFrame = data_helpers.get_stock_data(symbol.lower(), start_date=_start_date)
    if df_history.empty:
        return []

    df_history = df_history.reset_index()
    return df_history.to_dict("records")

@app.get("/api/v1/search", 
         response_model=list[SearchResult],
         tags=["Stock Data"])
async def search_stocks(q: str = ""):
    logger.info(f"[api/search_stocks] Getting supported stocks for keyword: {q}")
    support_stocks = config["data_sources"]["vn_stocks"]
    results = []
    for symbol in support_stocks:
        if q.upper() in symbol.upper():
            results.append(SearchResult(symbol=symbol))

    return results



