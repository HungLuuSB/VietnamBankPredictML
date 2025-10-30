import numpy as np

class LinearRegression:
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

    def fit(self, X: np.ndarray, y: np.ndarray):
        m, n = X.shape
        self.weights = np.zeros(n)
        for epoch in range(self.iters):
            y_pred = X @ self.weights
            error = y_pred - y
            grad = X.T @ error / m
            self.weights -= self.lr * grad
            
            if epoch % 50 == 0:
                self.cost_hist.append(self._cost(y, y_pred))

    @property
    def _coef(self):
        return self.weights

    @property
    def _cost_hist(self):
        return self.cost_hist

    def predict(self, X):
        return X @ self.weights


