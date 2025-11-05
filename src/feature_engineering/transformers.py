import pandas as pd

def simple_moving_average(seris: pd.Series, window: int) -> pd.Series:
    return seris.rolling(window=window, min_periods=1).mean() #type: ignore

def exponential_moving_average(seris: pd.Series, window: int) -> pd.Series:
    return seris.ewm(span=window, min_periods=1).mean() #type: ignore

def percentage_change(seris: pd.Series, periods: int = 1) -> pd.Series:
    return seris.pct_change(periods=periods)

def lag_features(seris: pd.Series, lag: int) -> pd.Series:
    return seris.shift(lag) #type: ignore

def target_variable(seris: pd.Series, horizon: int) -> pd.Series:
    return seris.shift(-horizon) #type: ignore
