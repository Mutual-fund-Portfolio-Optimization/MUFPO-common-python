from sklearn.preprocessing import MinMaxScaler, StandardScaler
import pandas as pd
from typing import Tuple
from statsmodels.tsa.seasonal import seasonal_decompose
from datetime import datetime


def fill_dataframe(df: pd.DataFrame, target_col: str, scale: bool=True) -> Tuple[pd.DataFrame, MinMaxScaler]:
    scaler = MinMaxScaler()
    df[target_col] = df[target_col].ffill().bfill()
    if scale:
        df[target_col] = scaler.fit_transform(df[[target_col]])
    return df, scaler

def fill_missing_date(df: pd.DataFrame, date_range: pd.DataFrame, merge_col: str = 'date'):
    return df.merge(date_range, on=merge_col, how='left')


class ExperimentData():
    def __init__(self, data_path: str, target: str, scale='minmax'):
        self.container: list
        self.scaler_container: dict
        self.df = pd.read_csv(data_path, index_col=0, parse_dates=True)
        self.target = target
        self.scale = scale
        self.df = self.preprocess()
        

    def get_train_test(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        train = self.df[self.df.year < 2023]
        test = self.df[self.df.year == 2023]
        return train, test

    def split_date(self, df: pd.DataFrame, date_col='date') -> pd.DataFrame:
        df = df.copy()  # Avoid modifying the original DataFrame
        df[date_col] = pd.to_datetime(df[date_col])
        df['week'] = df[date_col].dt.isocalendar().week
        df['month'] = df[date_col].dt.month
        df['quarter'] = df[date_col].dt.quarter
        df['year'] = df[date_col].dt.year

        return df

    def find_valid_fund(self, df: pd.DataFrame):
        return [group.fund_name.unique()[0] for _, group in df.groupby('fund_name') if group.date.min() <= datetime(day=1, month=1, year=2012)]

    def get_max_min_date(self, df: pd.DataFrame) -> Tuple[datetime, datetime]:
        min_date = max([group.date.min() for _, group in df.groupby('fund_name')])
        max_date = min([group.date.max() for _, group in df.groupby('fund_name')])
        return min_date, max_date
    
    def fill_fund_data(self, df: pd.DataFrame, date_range: pd.DataFrame) -> pd.DataFrame:
        self.container = []
        self.scaler_container = {}
        for key, group in df.groupby('fund_name'):
            group = group.sort_values('date')
            group = date_range.merge(group, on='date', how='left')
            group['fund_name'] = group['fund_name'].ffill().bfill()
            group[self.target] = group[self.target].replace(0, None)
            group[self.target] = group[self.target].ffill().bfill()
            if self.scale == 'minmax':
                self.scaler_container[key] = MinMaxScaler((10e-10,1))
                group[self.target] = self.scaler_container[key].fit_transform(group[[self.target]])
            group.index = pd.to_datetime(group['date'])
            result = seasonal_decompose(group[self.target], model='multiplicative', period=60)
            group['trend'] = result.trend
            group['seasonal'] = result.seasonal
            group['resid'] = result.resid
            self.container.append(group)
        df= pd.concat(self.container)
        return df
    
    def to_float32(self, df: pd.DataFrame) -> pd.DataFrame:
        df[self.target] = df[self.target].astype('float32')
        df['trend'] = df['trend'].astype('float32')
        df['seasonal'] = df['seasonal'].astype('float32')
        df['resid'] = df['resid'].astype('float32')
        return df

    def preprocess(self) -> pd.DataFrame:
        df = self.df[['fund_name', self.target, 'date']]
        df.date = pd.to_datetime(df.date)

        # filter valid fund
        valid = self.find_valid_fund(df)
        df = df[df.fund_name.isin(valid)]
        
        #filter date
        min_date, max_date = self.get_max_min_date(df)
        df = df[(df.date >= min_date) & (df.date <= max_date)]
        
        # create date dim
        date_range = pd.DataFrame(pd.date_range(start=min_date, end=max_date), columns=['date'])

        # fill data
        df = self.fill_fund_data(df, date_range)

        # set date to index
        df.index = df.date
        
        # check columns name
        try:
            df = df[['fund_name', self.target, 'trend', 'seasonal', 'resid', 'date']]
        except KeyError as error:
            raise KeyError(f'Columns is invalid it contains: {df.columns} the error with message {error}')
        
        df = self.to_float32(df)
        df = self.split_date(df)
        return df




