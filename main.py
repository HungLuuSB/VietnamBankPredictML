from data.crawler import Crawler
from data.models import CafefRawJson
from core.model_selection import PolynomialRegression

import json
import numpy as np

def main():
    url = 'https://cafef.vn/du-lieu/Ajax/PageNew/DataHistory/PriceHistory.ashx?Symbol=VCB&StartDate=&EndDate=&PageIndex=1&PageSize=10000'

    polynomial_regression = PolynomialRegression(degree=3)
    X_fit = polynomial_regression._design(np.array([0, 1, 2, 3]))
    print(X_fit)

if __name__ == "__main__":
    main()
