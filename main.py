from data.crawler import Crawler
from core.model_selection import PolynomialRegression

import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import datetime as dt

def main():
    url = 'https://cafef.vn/du-lieu/Ajax/PageNew/DataHistory/PriceHistory.ashx?Symbol=VCB&StartDate=01/01/2015&EndDate=&PageIndex=1&PageSize=10000'
    crawler = Crawler()
    response = crawler.Crawl(url)
    blob = json.loads(response.content)
    records = blob['Data']['Data']
    df = pd.json_normalize(records)
    df['Date'] = pd.to_datetime(df['Ngay'], format='%d/%m/%Y')
    df['x'] = df['Date'].map(dt.datetime.toordinal)
    start_date = df['x'].iloc[-1]
    print(start_date)
    df['x'] = df['x']
    df['y'] = pd.to_numeric(df['GiaDongCua'], errors='coerce')
    df = df[['Date','y','x']].dropna()
    X = df['x'][:-1]
    y = df['y'][:-1]
    poly_reg = PolynomialRegression(degree=8, learning_rate=1e-97, iterations=300000)
    coeffs = np.polyfit(X, y, deg=4)   # highest power first
    y_pred = np.polyval(coeffs, X)
    plt.scatter(X, y)
    plt.plot(X, y_pred, color='red', linewidth=2, label='Model Prediction')
    plt.show()

if __name__ == "__main__":
    main()
