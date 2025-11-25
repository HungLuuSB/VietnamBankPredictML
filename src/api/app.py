import sys
from pathlib import Path
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException, Query, Body
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from loguru import logger

BASE_DIR = Path(__file__).resolve().parents[2]
sys.path.append(str(BASE_DIR))

from src.utils.config_loader import config

pg_conn = None
TARGET_STOCKS = config['project']['target_stocks']

# --- LIFESPAN (STARTUP/SHUTDOWN) ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    global pg_conn
    try:
        db_conf = config['database']
        # Kết nối PostgreSQL
        pg_conn = psycopg2.connect(
            dbname=db_conf['postgres_url'].split('/')[-1],
            user=db_conf['postgres_user'],
            password=db_conf['postgres_pass'],
            host="localhost",
            port=5432
        )
        logger.info("✅API: Connected to PostgreSQL successfully.")
        yield
    except Exception as e:
        logger.critical(f"❌ API: Database connection failed: {e}")
        raise
    finally:
        if pg_conn:
            pg_conn.close()
            logger.info("API: Database connection closed.")

# --- APP INIT ---
app = FastAPI(
    title="Stock Prediction Big Data API",
    version="1.0.0",
    lifespan=lifespan
)

# CORS (Cho phép Streamlit/Web gọi vào)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def health_check():
    return {"status": "active", "service": "Stock Prediction API"}

@app.get("/supported-stocks")
def get_supported_stocks():
    return {"stocks": TARGET_STOCKS}

@app.get("/history/{stock_code}")
def get_stock_history(
    stock_code: str, 
    range: str = Query("1Y", enum=["7D", "1M", "3M", "6M", "1Y", "ALL"])
):
    if not pg_conn:
        raise HTTPException(status_code=503, detail="Database connection unavailable")

    interval_map = {
        "7D": "7 days", "1M": "1 month", "3M": "3 months", 
        "6M": "6 months", "1Y": "1 year", "ALL": "50 years"
    }
    interval = interval_map.get(range, "1 year")
    clean_table = "".join(c for c in stock_code if c.isalnum()).lower()
    try:
        cursor = pg_conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(
            "SELECT to_regclass(%s)", (clean_table,)
        )
        if cursor.fetchone()['to_regclass'] is None: # pyright: ignore
            raise HTTPException(status_code=404, detail=f"Stock code '{stock_code}' not found in database.")

        query = f"""
            SELECT date, open, high, low, close, volume 
            FROM {clean_table}
            WHERE date::DATE >= NOW() - INTERVAL %s
            ORDER BY date ASC
        """
        cursor.execute(query, (interval,))
        data = cursor.fetchall()
        cursor.close()
        return data

    except psycopg2.Error as e:
        pg_conn.rollback()
        logger.error(f"Database Error: {e}")
        raise HTTPException(status_code=500, detail="Internal Database Error")

@app.post("/predict")
def get_prediction(payload: dict = Body(...)):
    stock_code = payload.get("stock_code", "").upper()
    if not stock_code:
        raise HTTPException(status_code=400, detail="Missing stock_code")

    if not pg_conn:
        raise HTTPException(status_code=503, detail="Database unavailable")

    try:
        cursor = pg_conn.cursor(cursor_factory=RealDictCursor)
        query = """
            SELECT predicted_price, prediction_date 
            FROM stock_predictions 
            WHERE stock_code = %s 
            ORDER BY prediction_date DESC LIMIT 1
        """
        cursor.execute(query, (stock_code,))
        result = cursor.fetchone()
        cursor.close()

        if result:
            return {
                "stock_code": stock_code,
                "predicted_price": result['predicted_price'],
                "date": result['prediction_date']
            }
        else:
            return {"stock_code": stock_code, "predicted_price": None, "message": "Prediction not ready yet."}

    except Exception as e:
        pg_conn.rollback()
        logger.error(f"Prediction Error: {e}")
        return {"stock_code": stock_code, "predicted_price": None}

@app.get("/rankings")
def get_rankings(type: str = Query("tssl_1d")):
    if not pg_conn:
        raise HTTPException(status_code=503, detail="Database unavailable")
    try:
        cursor = pg_conn.cursor(cursor_factory=RealDictCursor)
        query = """
            SELECT symbol, metric_value, date
            FROM stock_rankings
            WHERE metric_type = %s
            ORDER BY metric_value DESC
            LIMIT 10
        """
        cursor.execute(query, (type,))
        data = cursor.fetchall()
        cursor.close()
        if not data:
            return []
        return data
    except Exception as e:
        pg_conn.rollback()
        logger.error(f"Ranking Error: {e}")
        raise HTTPException(status_code=500, detail=f"Internal DB Error: {e}")
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("src.api.app:app", host="127.0.0.1", port=8000, reload=True)
