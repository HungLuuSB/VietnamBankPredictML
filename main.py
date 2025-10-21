from data.crawler import Crawler
from data.models import CafefRawJson
import json

def main():
    url = 'https://cafef.vn/du-lieu/Ajax/PageNew/DataHistory/PriceHistory.ashx?Symbol=VCB&StartDate=&EndDate=&PageIndex=1&PageSize=10000'

    crawler = Crawler()
    respone = crawler.Crawl(url)
    if respone.status_code == 200:
        coded = json.loads(respone.content)
        obj = CafefRawJson.CafeFRawJson.from_json(coded)
        print(obj.Data)
    else:
        print(f"Error: {respone.status_code}")
    return

if __name__ == "__main__":
    main()
