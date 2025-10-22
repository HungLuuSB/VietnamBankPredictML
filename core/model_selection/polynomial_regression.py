import numpy as np

class PolynomialRegression:
    def __init__(self, degree: int = 2, learning_rate: float = 0.001, iterations: int = 10000):
        self.degree = degree
        self.iterations = iterations
        self.learning_rate = learning_rate
        self.weights = None
        self.loss_history = []

    def _design(self, X):
        return np.array([[x1 ** p for p in range(self.degree + 1)] for x1 in X])

    def fit(self, X, y):
        if len(X) != len(y):
            raise ValueError("x and y must have same length")
        if len(X) <= self.degree:
            raise ValueError("need more observations than polynomial degree")
        X_poly = self._design(X)
        n_samples, n_features = X_poly.shape
        self.weights = np.zeros(self.degree + 1)

        for i in range(self.iterations):
            y_pred = X_poly @ self.weights
            error = y_pred - y
            gradient = X_poly.T @ error / n_samples
            gradient = np.asarray(gradient, dtype=np.float64) 
            self.weights -= self.learning_rate * gradient
            mse = (error @ error) / n_samples
            self.loss_history.append(mse)
            if i % (self.iterations // 10) == 0 or i == 1:
                print(f"Iteration {i} | MSE: {mse:.8f} | Coefficients: {self.weights}")
        return self

    def predict(self, X):
        X_poly = self._design(X)
        return X_poly @ self.weights

