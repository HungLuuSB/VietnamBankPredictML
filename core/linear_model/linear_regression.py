from __future__ import annotations
from typing import Optional, Tuple, overload, Literal
import numpy as np
import pandas as pd


class LinearRegression:
    def __init__(self) -> None:
        self.coef_: Optional[np.ndarray] = None
        self.intercept: float = 0.0

    def fit(
        self, X: pd.DataFrame | np.ndarray, y: pd.Series | np.ndarray
    ) -> LinearRegression:
        X, y = self._validate_data(X, y)
        X_aug = np.column_stack([np.ones(X.shape[0]), X])
        beta = np.linalg.pinv(X_aug.T @ X_aug) @ X_aug.T @ y
        self.intercept_ = float(beta[0])
        self.coef_ = beta[1:]
        return self

    def predict(self, X: pd.DataFrame | np.ndarray) -> np.ndarray:
        if self.coef_ is None:
            raise ValueError("Model is not fitted yet.")
        X = self._validate_data(X, require_target=False)
        return X @ self.coef_ + self.intercept_

    @overload
    def _validate_data(
        self,
        X: pd.DataFrame | np.ndarray,
        y: pd.Series | np.ndarray,
        require_target: Literal[True] = ...,
    ) -> Tuple[np.ndarray, np.ndarray]: ...

    @overload
    def _validate_data(
        self,
        X: pd.DataFrame | np.ndarray,
        y: None = ...,
        require_target: Literal[False] = ...,
    ) -> np.ndarray: ...

    def _validate_data(
        self,
        X: pd.DataFrame | np.ndarray,
        y: Optional[pd.Series | np.ndarray] = None,
        require_target: bool = True,
    ) -> Tuple[np.ndarray, Optional[np.ndarray]]:
        if isinstance(X, pd.DataFrame):
            X = X.to_numpy(dtype=float)
        else:
            X = np.asarray(X, dtype=float)

        if X.ndim == 1:
            X = X.reshape(-1, 1)

        if require_target:
            if y is None:
                raise ValueError("y is required for fitting.")
            if isinstance(y, pd.Series):
                y = y.to_numpy(dtype=float)
            else:
                y = np.asarray(y, dtype=float)
            if y.ndim != 1:
                raise ValueError("y must be 1-D")
            if X.shape[0] != y.shape[0]:
                raise ValueError("X and y must have same number of samples")
            return X, y
        return X, None

    def __repr__(self) -> str:
        return f"LinearRegression(intercept={self.intercept_:.4f}, coef={self.coef_})"
