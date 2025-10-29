from itertools import product
import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from core.model_selection import LinearRegression

def _transform(X):
    return X[0] * 2 + X[1] * 1.5 + X[2] * 0.75

def main():
    #url = 'https://cafef.vn/du-lieu/Ajax/PageNew/DataHistory/PriceHistory.ashx?Symbol=AGR&StartDate=01/01/2015&EndDate=&PageIndex=1&PageSize=10000'
    X = np.array([
        [1, 2, 3],
        [2, 3, 4],
        [3, 4, 5],
        [4, 5, 6],
        [5, 6, 7],
        [6, 7, 8],
        [7, 8, 9]
    ])
    
    x_test = np.array([
        [10, 11, 12],
        [15, 16, 17]
    ])

    y = np.array([_transform(x) for x in X])
    model = LinearRegression(iters=100000, lr = 0.01)
    model.fit(X, y)
    print(model.coeff_)
    y_pred = model.predict(x_test)
    print(y_pred)

if __name__ == "__main__":
    main()
