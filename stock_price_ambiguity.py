from pandas.errors import EmptyDataError
from argparse import ArgumentParser
from multiprocessing import Pool
from datetime import timedelta
from utils import File_manager
from scipy.stats import norm
from sqlite3 import connect
from parmap import map
import pandas as pd
import numpy as np


bins = delta = cursor = None
db_path = './data/raw\\stockPrice1.db'


def init():
    global bins, delta, cursor
    delta = timedelta(minutes=5)
    bins = np.linspace(-0.1, 0.1, 41)
    cursor = connect(db_path).cursor()


def worker(stock, t):
    price = get_price(stock, t)
    return get_ambiguity_series(stock, price, delta, bins)


def get_price(stock, t):
    query = cursor.execute(
        f'SELECT date, open, close, volume FROM {stock} WHERE date > {t}'
        )
    price = pd.DataFrame.from_records(data=query.fetchall())

    if price.empty:
        return None

    price = price[price[3] > 0]
    price[0] = pd.to_datetime(price[0], format='%Y%m%d%H%M')
    price.set_index(0, inplace=True)

    return price


def get_monthly_ambiguity(day_list, delta, bins):
    bin_prop = []

    for day in day_list:
        estimates = {}
        t = day.index[0]
        lagged = day.shift(-1)
        lagged[1][-1] = day[2][-1]
        np.log_returns = (np.log(lagged[1]) - np.log(day[1]))

        for time in day.index:
            if t == time:
                t += delta
            else:
                omitted = []
                t1 = t - delta
                while t != time:
                    omitted.append(t)
                    t += delta
                t2 = t
                t += delta

                r = day.loc[t1][3] * np.log_returns[t1]
                r += day.loc[t2][3] * np.log_returns[t2]
                r /= day.loc[t1][3] + day.loc[t2][3]

                if abs(r) < 0.1:
                    estimates.update({k: r for k in omitted})

        s = pd.Series(estimates)
        np.log_returns = pd.concat([s, np.log_returns])
        np.log_returns = np.log_returns[abs(np.log_returns) < 0.1]

        n = len(np.log_returns)
        mean = np.log_returns.mean()
        adj = np.log_returns - mean
        var = np.log_returns.var(ddof=0)
        var += 2 * (adj * adj.shift(-1))[:-1].sum() / (n - 1)
        var = var if var > 0 else 0

        if var:
            rv = norm(loc=mean, scale=var ** 0.5)
            cdf = rv.cdf(bins)
            pdf = np.concatenate([[cdf[0]], np.diff(cdf), [1 - cdf[-1]]])
        else:
            pdf = [
                1 if mean > i and mean <= i + 0.005 else 0 for i in bins
                ]
            pdf[-1] = 1 if mean > bins[-1] else 0
            pdf = np.concatenate([[1 if mean <= bins[0] else 0], pdf])

        bin_prop.append(pdf)

    bin_prop = np.array(bin_prop)

    return sum([
        bin_prop[:, i].mean() * bin_prop[:, i].var() for i in range(42)
        ]) / (0.005 * np.log(200))


def get_ambiguity_series(stock, price, delta, bins):
    ambiguity = {}

    if price is None:
        return pd.Series(ambiguity, name=stock)

    monthly_group = price.groupby(pd.Grouper(freq='M'))

    for i in monthly_group:
        daily_group = i[1].groupby(pd.Grouper(freq='D'))
        day_list = []

        for j in daily_group:
            if len(j[1]) > 14:
                day_list.append(j[1])

        if len(day_list) > 14:
            time = int(i[0].strftime('%Y%m'))
            ambiguity[time] = get_monthly_ambiguity(day_list, delta, bins)

    return pd.Series(ambiguity, name=stock)


def stock_price_ambiguity(max_workers):
    cur = connect(db_path).cursor()
    cur.execute('SELECT name FROM sqlite_master WHERE type="table"')
    stock_list = [row[0] for row in cur]
    cur.close()

    result = File_manager('analyzed', 'ambiguity')
    t = int(result.ver['ambiguity']) * 1000000

    try:
        result_df = pd.read_csv(result.path, index_col=0)
    except EmptyDataError:
        result_df = None

    with Pool(processes=int(max_workers), initializer=init) as p:
        stock_ambiguity = map(worker, stock_list, t, pm_pool=p, pm_pbar=True)
    stock_ambiguity = pd.concat(stock_ambiguity, axis=1)

    if result_df is not None:
        stock_ambiguity = pd.concat([result_df.iloc[:-1], stock_ambiguity])

    stock_ambiguity.sort_index(axis=0, inplace=True)

    result.update_version({'ambiguity': stock_ambiguity.index[-1]})
    stock_ambiguity.to_csv(result.path)


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('-m', '--max_workers', required=False, default='6')
    args = parser.parse_args()

    stock_price_ambiguity(args.max_workers)
