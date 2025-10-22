from . import StockData
import json

class Stock:
    def __init__(self, id: str, name: str, stock_data: list[StockData]) -> None:
        self.id = id
        self.name = name
        self.stock_data = stock_data

    def __str__(self) -> str:
        return json.dumps(self.__dict__)
        
