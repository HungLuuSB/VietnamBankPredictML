import requests

class Crawler:
    def Crawl(self, url: str) -> requests.Response:
        response = requests.get(url)
        return response
