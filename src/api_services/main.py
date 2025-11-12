from contextlib import asynccontextmanager
import sys
import json
import pandas as pd
import numpy as np
from pathlib import Path

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Query
from loguru import logger
from datetime import datetime
from dateutil.relativedelta import relativedelta

file_path = Path(__file__).resolve()
project_root = file_path.parent.parent.parent
sys.path.append(str(project_root))

ARTIFACTS_PATH = project_root / 'artifacts'

from src.feature_engineering.scalers import StandardScaler
from src.model_core.implementations.linear_regression import LinearRegression
from src.utils.config_loader import config
from src.data_ingestion import data_helpers
from src.feature_engineering import features_engineering_helpers
from src.utils.config_loader import config
from .schemas import StockDataPoint, SearchResult, PredictionMessage
from aiokafka import AIOKafkaConsumer

global_artifacts = {}

def load_artifacts():
    vn_stock_codes = config["data_sources"]["vn_stocks"]

    for symbol in vn_stock_codes:
        symbol = symbol.lower()
        model_path = ARTIFACTS_PATH / f"{symbol}_lr_scratch.npz"
        scaler_path = ARTIFACTS_PATH / f"{symbol}_scaler.npz"
        
        if not model_path.exists() or not scaler_path.exists():
            logger.warning(f"[FastAPI] Could not find model_path or scaler_path for {symbol.upper()}")
            continue

        try:
            scaler = np.load(scaler_path)
            model_data = np.load(model_path)
            w = model_data["weights"]
            b = model_data["bias"].item()
            global_artifacts[symbol] = {
                "scaler": scaler,
                "weights": w,
                "bias": b
            }
            logger.success(f"[FastAPI] Successfully loaded artifacts for {symbol.upper()}:\n\tWeight: {w}\n\tBias: {b}")
        except Exception as e:
            logger.error(f"[FastAPI] Error while loading artifacts for {symbol.upper()}: {e}")

    print(global_artifacts)

@asynccontextmanager
async def lifespan(app: FastAPI):
    load_artifacts()
    yield
    global_artifacts.clear()

app = FastAPI(
    title="VietnamStock",
    description="Stock view and prediction.",
    lifespan=lifespan
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

@app.get("/api/v1/predict",
         response_model=PredictionMessage,
         tags=["Stock Data"])
async def predict(symbol: str):
    logger.info(f"[api/predict] Getting prediction for {symbol}")
    if symbol.lower() not in global_artifacts:
        raise HTTPException(
            status_code=404,
            detail=f"{symbol} is not suppreted."
        )
    try:
        model_path = str(ARTIFACTS_PATH / f"{symbol.lower()}_lr_scratch.npz")
        scaler_path = str(ARTIFACTS_PATH / f"{symbol.lower()}_scaler.npz")
        X = np.asarray(features_engineering_helpers.process_today_features(symbol))
        scaler = StandardScaler()
        scaler.load_scaler(scaler_path)
        X_scaled = scaler.transform(X)
        model = LinearRegression()
        model.load_model(model_path)
        y_pred = model.predict(X_scaled)
        return {
            "prediction": y_pred
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error while performing predicting process: {str(e)}"
        )

