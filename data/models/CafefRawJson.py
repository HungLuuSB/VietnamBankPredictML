import json

from requests.cookies import MockResponse
class StockData:
    def __init__(self, ngay, giaDieuChinh, giaDongCua, thayDoi, khoiLuongKhopLenh, giaTriKhopLenh, khoiLuongThoaThuan, giaTriThoaThuan, giaMoCua, giaCaoNhat,  giaThapNhat) -> None:
        self.Ngay = ngay
        self.GiaDieuChinh = giaDieuChinh
        self.GiaDongCua = giaDongCua
        self.ThayDoi = thayDoi
        self.KhoiLuongKhopLenh = khoiLuongKhopLenh
        self.GiaTriKhopLenh = giaTriKhopLenh
        self.KhoiLuongThoaThuan = khoiLuongThoaThuan
        self.GiaTriThoaThuan = giaTriThoaThuan
        self.GiaMoCua = giaMoCua
        self.GiaCaoNhat = giaCaoNhat
        self.GiaThapNhat = giaThapNhat

    def __str__(self):
        return json.dumps(self.__dict__)

    @classmethod
    def from_json(cls, json_data: str):
        if isinstance(json_data, str):
            data = json.loads(json_data)
        else:
            data = json_data
        return cls(
            data['Ngay'],
            data['GiaDieuChinh'],
            data['GiaDongCua'],
            data['ThayDoi'],
            data['KhoiLuongKhopLenh'],
            data['GiaTriKhopLenh'],
            data['KLThoaThuan'],
            data['GtThoaThuan'],
            data['GiaMoCua'],
            data['GiaCaoNhat'],
            data['GiaThapNhat']
        )

class StockDataHeader:
    def __init__(self, totalCount: int, data: list[StockData]) -> None:
        self.TotalCount = totalCount
        self.Data = data

    def __str__(self) -> str:
        result = ""
        for item in self.Data:
            result += item.__str__();
        return result

    @classmethod
    def from_json(cls, json_data: str):
        if isinstance(json_data, str):
            data = json.loads(json_data)
        else:
            data = json_data
        Data = []
        stock_datas = data['Data']
        for item in stock_datas:
            Data.append(StockData.from_json(item))
        return cls(data['TotalCount'], Data)


class CafeFRawJson:
    def __init__(self, data: StockDataHeader, message: str, success: str) -> None:
        self.Data = data
        self.Message = message
        self.Success = success

    @classmethod
    def from_json(cls, json_data: str):
        if isinstance(json_data, str):
            data = json.loads(json_data)
        else:
            data = json_data
        Data = StockDataHeader.from_json(data['Data'])
        return cls(Data, data['Message'], data['Success'])

