import modin.pandas as mpd  # Importação do Modin
import numpy as np

def read_file_parquet(df=None):
    return mpd.read_parquet('/dbfs/FileStore/ks_taxi_parquet', index='index')

def count(df=None):
    return len(df)

def count_index_length(df=None):
    return len(df.index)

def mean(df):
    return df.fare_amt.mean()

def standard_deviation(df):
    return df.fare_amt.std()

def mean_of_sum(df):
    return (df.fare_amt + df.tip_amt).mean()

def sum_columns(df):
    return (df.fare_amt + df.tip_amt)

def mean_of_product(df):
    return (df.fare_amt * df.tip_amt).mean()

def product_columns(df):
    return (df.fare_amt * df.tip_amt)

def value_counts(df):
    return df.fare_amt.value_counts()

def mean_of_complicated_arithmetic_operation(df):
    theta_1 = df.start_lon
    phi_1 = df.start_lat
    theta_2 = df.end_lon
    phi_2 = df.end_lat
    temp = (np.sin((theta_2-theta_1)/2*np.pi/180)**2
           + np.cos(theta_1*np.pi/180)*np.cos(theta_2*np.pi/180) * np.sin((phi_2-phi_1)/2*np.pi/180)**2)
    ret = 2 * np.arctan2(np.sqrt(temp), np.sqrt(1-temp))
    return ret.mean()

def complicated_arithmetic_operation(df):
    theta_1 = df.start_lon
    phi_1 = df.start_lat
    theta_2 = df.end_lon
    phi_2 = df.end_lat
    temp = (np.sin((theta_2-theta_1)/2*np.pi/180)**2
           + np.cos(theta_1*np.pi/180)*np.cos(theta_2*np.pi/180) * np.sin((phi_2-phi_1)/2*np.pi/180)**2)
    ret = 2 * np.arctan2(np.sqrt(temp), np.sqrt(1-temp))
    return ret

def groupby_statistics(df):
    return df.groupby(by='passenger_count').agg(
      {
        'fare_amt': ['mean', 'std'],
        'tip_amt': ['mean', 'std']
      }
    )

def join_count(df, other):
    return len(df.merge(other, left_index=True, right_index=True))

def join_data(df, other):
    return df.merge(other, left_index=True, right_index=True)