import numpy as np

class StandardScaler:
    def __init__(self) -> None:
        self.mean_ = None
        self.scale_ = None

    def fit(self, X: np.ndarray) -> None:
        self.mean_ = np.mean(X, axis=0)
        self.scale_ = np.std(X, axis=0)

        self.scale_ = np.where(self.scale_ == 0, 1, self.scale_)

    def transform(self, X: np.ndarray) -> np.ndarray:
        if self.mean_ is None or self.scale_ is None:
            raise Exception("fit() model first.")

        return (X - self.mean_) / self.scale_

    def fit_transform(self, X: np.ndarray):
        self.fit(X)
        return (X - self.mean_) / self.scale_

    def inverse_transform(self, X_scaled: np.ndarray) -> np.ndarray:
        if self.mean_ is None or self.scale_ is None:
            raise Exception("fit() model first.")
        return (X_scaled * self.scale_) + self.mean_

