from pydantic import BaseModel
from datetime import datetime

class StockDataPoint(BaseModel):
    date: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int

class SearchResult(BaseModel):
    symbol: str

class PredictionMessage(BaseModel):
    symbol: str
    prediction: float
    prediction_horizon_days: int
    prediction_timestamp: datetime

