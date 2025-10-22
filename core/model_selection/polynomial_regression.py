import numpy as np

class PolynomialRegression:
    def __init__(self, degree: int = 2, fit_intercept: bool = True):
        self.degree = degree

    def _design(self, X: list[float] | np.ndarray):
        return np.array([[x1 ** p for p in range(self.degree + 1)] for x1 in X])

    def fit(self, X, y):
        pass

    def predict(self, y):
        pass
