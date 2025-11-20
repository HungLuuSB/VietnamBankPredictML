from abc import ABC, abstractmethod
from datetime import date
import pandas as pd
class BaseCollectorModel(ABC):
    @abstractmethod
    def fetch_data(self, symbol: str, start_date: str = "2010-01-01", end_date: str = date.today().isoformat()) -> pd.DataFrame:
        pass
