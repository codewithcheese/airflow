import numpy as np


class TsaDefault(object):

    """
    Defines a class that can be re-used to process data
    """

    def __init__(self, data, outliers):
        self.data = data
        self.x = [int(x.strftime('%s')) for x, y in data]
        self.index_of = {x: i for i, x in enumerate(self.x)}
        self.y = [y for x, y in data]
        self.outliers = outliers or []
        if len(outliers):
            self.outliers = [int(x.strftime('%s')) for x, y in outliers]

    def infer(self, xi):
        """
        infers yi given xi

        Override to modify how data is predicted
        """
        degree = 6
        window = 14
        x, y = [], []
        i = self.index_of[xi]
        for xi, yi in zip(self.x[i-window:i], self.y[i-window:i]):
            if not xi in self.outliers:
                x.append(xi)
                y.append(yi)
        if len(x) < degree + 1:
            return np.nan
        p = np.polyfit(x, y, degree)
        poly = np.poly1d(p)
        return poly(xi)

    def detrend(self):
        """
        De-trends using a polynomial fit to predict prior datapoint
        """
        return [(self.data[i][0], self.infer(xi))
                for i, xi in enumerate(self.x)]


def detrend(data, outliers):
    """
    Use the class defined above
    """
    dt = TsaDefault(data, outliers)
    return dt.detrend()
