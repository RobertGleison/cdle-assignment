import pandas as pd
import numpy as np
import dask.dataframe as dd

def read_file_parquet(df=None, **kwargs):
    return dd.read_parquet(kwargs.get("path"), index_col = 'index')

def count(df=None):
    return len(df)

def count_index_length(df=None):
    return len(df.index)

def mean(df):
    return df.Fare_Amt.mean().compute()

def standard_deviation(df):
    return df.Fare_Amt.std().compute()

def mean_of_sum(df):
    return (df.Fare_Amt + df.Tip_Amt).mean().compute()

def sum_columns(df):
    return (df.Fare_Amt + df.Tip_Amt).compute()

def mean_of_product(df):
    return (df.Fare_Amt * df.Tip_Amt).mean().compute()

def product_columns(df):
    return (df.Fare_Amt * df.Tip_Amt).compute()

def value_counts(df):
    return df.Fare_Amt.value_counts().compute()

def mean_of_complicated_arithmetic_operation(df):
    theta_1 = df.Start_Lon
    phi_1 = df.Start_Lat
    theta_2 = df.End_Lon
    phi_2 = df.End_Lat
    temp = (np.sin((theta_2-theta_1)/2*np.pi/180)**2
           + np.cos(theta_1*np.pi/180)*np.cos(theta_2*np.pi/180) * np.sin((phi_2-phi_1)/2*np.pi/180)**2)
    ret = 2 * np.arctan2(np.sqrt(temp), np.sqrt(1-temp))
    return ret.mean().compute()

def complicated_arithmetic_operation(df):
    theta_1 = df.Start_Lon
    phi_1 = df.Start_Lat
    theta_2 = df.End_Lon
    phi_2 = df.End_Lat
    temp = (np.sin((theta_2-theta_1)/2*np.pi/180)**2
           + np.cos(theta_1*np.pi/180)*np.cos(theta_2*np.pi/180) * np.sin((phi_2-phi_1)/2*np.pi/180)**2)
    ret = 2 * np.arctan2(np.sqrt(temp), np.sqrt(1-temp))
    return ret.compute()

def groupby_statistics(df):
    return df.groupby(by='Passenger_Count').agg(
      {
        'Fare_Amt': ['mean', 'std'],
        'Tip_Amt': ['mean', 'std']
      }
    ).compute()

def join_count(df, other):
    return len(dd.merge(df, other, left_index=True, right_index=True))

def join_data(df, other):
    return dd.merge(df, other, left_index=True, right_index=True).compute()