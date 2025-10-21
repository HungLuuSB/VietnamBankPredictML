from data.crawler import Crawler

def main():
    url = 'https://cafef.vn/du-lieu/Ajax/PageNew/DataHistory/PriceHistory.ashx?Symbol=VCB&StartDate=&EndDate=&PageIndex=1&PageSize=10000'

    crawler = Crawler()
    respone = crawler.Crawl(url)
    if respone.status_code == 200:
        print(respone.content)
    else:
        print(f"Error: {respone.status_code}")
    return

if __name__ == "__main__":
    main()
