
def error(y1, y2):
    """
    http://en.wikipedia.org/wiki/Symmetric_mean_absolute_percentage_error
    """
    return [math.fabs(y1-y2)/(math.fabs(y1) + math.fabs(y2)) \
     for xi, yi in zip(x, y)]

def detrend(df):
    y0 = df[df.columns[0]]
    yf = detrend(y)
    delta = error(y0, yf)
    df['Trend'] = yf[idx]
    df['Error'] = delta[idx]