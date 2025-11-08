import sys
import pandas as pd
import numpy as np
from pathlib import Path

file_path = Path(__file__).resolve()
project_root = file_path.parent.parent
sys.path.append(str(project_root))

from loguru import logger
from src.utils.config_loader import config
from src.model_core.implementations.linear_regression import LinearRegression
from src.data_ingestion.storage import SQLStorage
from src.feature_engineering.scalers import StandardScaler
START_DATE = config["project"]["start_date"]
END_DATE = config["project"]["end_date"]

def split_data(df: pd.DataFrame, target_column: str, test_size: float = 0.2):
    logger.info(f"[data_split] Splitting data test_size={test_size}")
    
    y = df[target_column].values
    X = df.drop(columns=[target_column]).values
    
    split_index = int(len(X) * (1 - test_size))
    
    X_train = X[:split_index]
    y_train = y[:split_index]
    X_test = X[split_index:]
    y_test = y[split_index:]

    print(X_train, y_train)
    
    logger.success(f"[data_split] Finished splitting data: Train {X_train.shape}, Test {X_test.shape}")
    return X_train, y_train, X_test, y_test

def training():
    logger.info(f"[scripts/data_ingestion] Starting data ingestion pipeline.")
    target_stock = config['project']['target_stock']
    processed_table_name = f"processed_{target_stock.lower()}_features"
    
    storage = SQLStorage(db_uri=config['database']['uri'])
    df_features = storage.load_data(processed_table_name, start_date=START_DATE, end_date=END_DATE)
    X_train, y_train, X_test, y_test = split_data(df_features, target_column="target_price")

    scaler = StandardScaler()
    scaler.fit(X_train)
    scaler.save_scaler(f"artifacts/{target_stock.lower()}_scaler.npz")
    
    X_train_scaled = scaler.transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    print(X_train_scaled[:5])
    print(y_train[:5])
    
    model = LinearRegression(lr=0.01, iters=100000)
    
    logger.info("[script/training] Starting training model...")
    model.fit(X_train_scaled, y_train) #type: ignore
    logger.success("[script/training] Finished training model.")
    
    logger.info("[script/training] Evaluating model on Test data...")
    evaluation_results = model.evaluate(X_test_scaled, y_test) #type: ignore
    
    logger.info(evaluation_results)
    logger.info(f"[linear_regression] Model coefs: {model.weights}, bias: {model.bias}")
    Path("artifacts").mkdir(parents=True, exist_ok=True)
    model.save_model(f"artifacts/{target_stock.lower()}_lr_scratch.npz")


    logger.info(f"[scripts/training] Finished model training pipeline.")

if __name__ == "__main__":
    training()
