
# DESCRIPTION OF THE TASK:
# Imagine that you have hourly history of electricity spot prices
# Here is your code generating such series
import pandas as pd
import polars as pl
import numpy as np

start_date = '2017-01-01 00:00:00'
end_date = '2023-12-31 23:00:00'
index = pd.date_range(start=start_date, end=end_date, freq='H')
data = np.random.uniform(low=50, high=100, size=len(index))

hourly_prices = pd.Series(data=data, index=index)
hourly_prices_pl = pl.DataFrame({'timestamp': index, 'price': data})

### PRINT FOLLOWING
# 1. MAX of HOURLY price for the year 2021
max(hourly_prices[hourly_prices.index.year == 2021])

# polars
hourly_prices_pl.filter(pl.col('timestamp').dt.year() == 2021).max()

# 2. MIN of average DAILY price for the whole period
min(hourly_prices.resample('D').mean())

# polars
(hourly_prices_pl
 .set_sorted(column="timestamp")
 .group_by_dynamic("timestamp", every="1d")
 .agg(pl.col("price").mean())
 .min()
 )

# 3a. average MONTHLY price for each year
x = hourly_prices.groupby([index.year, index.month]).mean()

# polars
x_pl = (hourly_prices_pl
 .set_sorted(column="timestamp")
 .group_by_dynamic("timestamp", every="1mo")
 .agg(pl.col("price").mean())
 )

# 3b. from 3a create pd.DataFrame where rows represent months and columns represent years
x.unstack()

# polars
(x_pl
 .with_columns((pl.col('timestamp').dt.year()).alias('year'))
 .with_columns((pl.col('timestamp').dt.month()).alias('month'))
 .pivot(values='price', index='month', columns='year')
)
