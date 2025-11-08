import numpy as np
from ..base_model import BaseModel
from .. import metrics
from loguru import logger
class LinearRegression(BaseModel):
    def __init__(self, lr: float = 0.1, iters: int = 10000, tol: float = 1e-3, patience: int = 10) -> None:
        self.lr = lr
        self.iters = iters
        self.tol = tol
        self.patience = patience
        self.weights = None
        self.bias = None
        self.cost_hist = []

    def fit(self, X: np.ndarray, y: np.ndarray) -> None:
        n_samples, n_features = X.shape
        self.weights = np.zeros(n_features)
        self.bias = 0.0
        best_loss = np.inf
        patience_counter = 0
        for epoch in range(self.iters):
            y_pred = self.predict(X)
            dw = (1 / n_samples) * np.dot(X.T, (y_pred - y))
            db = (1 / n_samples) * np.sum(y_pred - y)
            self.weights -= self.lr * dw
            self.bias -= self.lr * db
            if epoch % 50 == 0:
                loss = metrics.mean_squared_error(y, y_pred)
                logger.debug(f"Iteration {epoch+1}/{self.iters}, Loss (MSE): {loss:.4f}")
                self.cost_hist.append(loss)
                loss_change = best_loss - loss
                if loss_change > self.tol:
                    best_loss = loss
                    patience_counter = 0
                else:
                    patience_counter +=1

                if patience_counter >= self.patience:
                    logger.debug(f"Model Training: Early Stopping Encountered")
                    break

    def predict(self, X) -> np.ndarray:
        if self.weights is None or self.bias is None:
            raise Exception("Model has not yet been build. Build the model first by calling '.fit()' with some data.")
        return np.dot(X, self.weights) + self.bias
    
    def evaluate(self, X_test: np.ndarray, y_test: np.ndarray) -> dict:
        y_pred = self.predict(X_test)
        return {
            "mae": metrics.mean_absolute_error(y_test, y_pred),
            "mse": metrics.mean_squared_error(y_test, y_pred),
            "rsme": metrics.root_mean_squared_error(y_test, y_pred),
            "r2": metrics.r2_score(y_test, y_pred)
        }
    
    def save_model(self, filepath: str) -> None:
        if self.weights is None or self.bias is None:
            logger.error(f"[linear_regression] Could not save unfitted model.")
            return
        logger.info(f"[linear_regression] Saving model into {filepath}")
        np.savez(filepath, weights = self.weights, bias = self.bias) # type: ignore

    def load_model(self, filepath: str) -> None:
        logger.info(f"[linear_regression] Loading model from {filepath}")
        try:
            data = np.load(filepath)
            self.weights = data['weights']
            self.bias = data['bias'].item()
            logger.success(f"[linear_regression] Successfully loaded model from {filepath}")
        except FileNotFoundError:
            logger.error(f"[linear_regression] Could not find file: {filepath}")
            raise
        except KeyError as e:
            logger.error(f"[linear_regression]: File .npz does not contain enough keys (weights/bias): {e}")
            raise


