from loguru import logger
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

    def save_scaler(self, filepath: str):
        if self.mean_ is None or self.scale_ is None:
            logger.error(f"Model has not been fit().")
            return

        logger.info(f"[StandardScaler] Saving scale_ and mean_ into {filepath}")
        np.savez(filepath, mean=self.mean_, scale = self.scale_)


    def load_scaler(self, filepath: str):
        logger.info(f"[StandardScaler] Loading scale_ and mean_ from {filepath}")
        try:
            data = np.load(filepath)
            self.scale_ = data["scale"]
            self.mean_ = data["mean"]
            logger.success(f"[StandardScaler] Successfully loaded scaler: {self.scale_}, mean: {self.mean_} from {filepath}")
        except FileNotFoundError:
            logger.error(f"[StandardScaler] File not found {filepath}")
            raise
        except KeyError:
            logger.error(f"[StandardScaler] File .npz does not contain mean and scale value.")
            raise
