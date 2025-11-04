import numpy as np

def mean_absolute_error(y_true: np.ndarray, y_pred: np.ndarray):
    y_true, y_pred = np.asarray(y_true), np.asarray(y_pred)
    return float(np.mean(np.abs(y_true - y_pred)))

def mean_squared_error(y_true: np.ndarray, y_pred: np.ndarray):
    y_true, y_pred = np.asarray(y_true), np.asarray(y_pred)
    return float(np.mean((y_true - y_pred) ** 2))

def root_mean_squared_error(y_true: np.ndarray, y_pred: np.ndarray):
    return np.sqrt(mean_squared_error(y_true, y_pred))

def r2_score(y_true: np.ndarray, y_pred: np.ndarray):
    y_true, y_pred = np.asarray(y_true), np.asarray(y_pred)
    ss_res = np.sum((y_true - y_pred) ** 2)
    ss_tot = np.sum((y_true - np.mean(y_true)) ** 2)

    if ss_tot == 0:
        return 1.0 if ss_res == 0 else 0.0
    return 1.0 - (ss_res / ss_tot)
