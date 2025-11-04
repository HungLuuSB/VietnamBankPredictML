import numpy as np
from ..base_model import BaseModel
from .. import metrics
class LinearRegression(BaseModel):
    def __init__(self, lr: float = 0.1, fit_intercept: bool = True, iters: int = 10000) -> None:
        self.lr = lr
        self.iters = iters
        self.fit_intercept = fit_intercept
        self.weights = None
        self.cost_hist = []

    def _grad(self, w, x):
        return x * w

    def _cost(self, y_true: np.ndarray, y_pred: np.ndarray):
        return np.mean((y_pred - y_true) ** 2) / 2

    def fit(self, X: np.ndarray, y: np.ndarray) -> None:
        m, n = X.shape
        self.weights = np.zeros(n)
        for epoch in range(self.iters):
            y_pred = X @ self.weights
            error = y_pred - y
            grad = X.T @ error / m
            self.weights -= self.lr * grad
            
            if epoch % 50 == 0:
                self.cost_hist.append(self._cost(y, y_pred))

    def predict(self, X) -> np.ndarray:
        return X @ self.weights
    
    def evaluate(self, X_test: np.ndarray, y_test: np.ndarray) -> dict:
        y_pred = self.predict(X_test)
        return {
            "mae": metrics.mean_absolute_error(y_test, y_pred),
            "mse": metrics.mean_squared_error(y_test, y_pred),
            "rsme": metrics.root_mean_squared_error(y_test, y_pred),
            "r2": metrics.r2_score(y_test, y_pred)
        }
    
    def save_model(self, filepath: str) -> None:
        print(f"Saving model into {filepath}")
        np.savez(filepath, weights = self.weights) # type: ignore

    def load_model(self, filepath: str) -> None:
        print(f"Loading model from {filepath}")
        data = np.load(filepath)
        self.weights = data['weights']

    @property
    def _coef(self):
        return self.weights

    @property
    def _cost_hist(self):
        return self.cost_hist



