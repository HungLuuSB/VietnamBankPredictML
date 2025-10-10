class LinearRegression:
    def __init__(
        self, fit_intercept: bool = True, n_jobs: int = 10000, tol: float = 1e-6
    ) -> None:
        self.intercept = fit_intercept
        self.n_jobs = n_jobs
        self.tol = tol

    def fit(self, X, y):
        return self

    def predict(self):
        pass

    def __repr__(self) -> str:
        return ""
