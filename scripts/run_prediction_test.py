import sys
import pandas as pd
import numpy as np
from pathlib import Path

from loguru import logger

file_path = Path(__file__).resolve()
project_root = file_path.parent.parent
sys.path.append(str(project_root))

from src.utils.config_loader import config
from src.feature_engineering.scalers import StandardScaler
from src.model_core.implementations import LinearRegression
from src.data_ingestion import data_helpers

def main():
    target_stock = config['project']['target_stock']
    logger.info(f"[scripts/run_prediction_test] Loading model from file")
    model = LinearRegression()
    model.load_model(f"artifacts/{target_stock.lower()}_lr_scratch.npz")

    print(model.weights, model.bias)
    X = np.array([59.3, 6728.7998046875, 46987.1015625, 3999.39990234375, 59.75, 60.55, 61.5582306820914, -0.0165837479270315, 60.3])
    scaler = StandardScaler()
    scaler.load_scaler(f"artifacts/{target_stock.lower()}_scaler.npz")
    X_scaled = scaler.transform(X)
    print(X_scaled)

    y_pred = model.predict(X_scaled)
    print(y_pred)



if __name__ == "__main__":
    main()
