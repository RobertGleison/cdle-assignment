import modin.pandas as mpd  # Importação do Modin
import numpy as np

def read_file_parquet(df=None, **kwargs):
    return mpd.read_parquet(kwargs.get("path"))

def count(df=None):
    return len(df)

def count_index_length(df=None):
    return len(df.index)

def mean(df):
    return df.Fare_Amt.mean()

def standard_deviation(df):
    return df.Fare_Amt.std()

def mean_of_sum(df):
    return (df.Fare_Amt + df.Tip_Amt).mean()

def sum_columns(df):
    return (df.Fare_Amt + df.Tip_Amt)

def mean_of_product(df):
    return (df.Fare_Amt * df.Tip_Amt).mean()

def product_columns(df):
    return (df.Fare_Amt * df.Tip_Amt)

def value_counts(df):
    return df.Fare_Amt.value_counts()

def mean_of_complicated_arithmetic_operation(df):
    theta_1 = df.Start_Lon
    phi_1 = df.Start_Lat
    theta_2 = df.End_Lon
    phi_2 = df.End_Lat
    temp = (np.sin((theta_2-theta_1)/2*np.pi/180)**2
           + np.cos(theta_1*np.pi/180)*np.cos(theta_2*np.pi/180) * np.sin((phi_2-phi_1)/2*np.pi/180)**2)
    ret = 2 * np.arctan2(np.sqrt(temp), np.sqrt(1-temp))
    return ret.mean()

def complicated_arithmetic_operation(df):
    theta_1 = df.Start_Lon
    phi_1 = df.Start_Lat
    theta_2 = df.End_Lon
    phi_2 = df.End_Lat
    temp = (np.sin((theta_2-theta_1)/2*np.pi/180)**2
           + np.cos(theta_1*np.pi/180)*np.cos(theta_2*np.pi/180) * np.sin((phi_2-phi_1)/2*np.pi/180)**2)
    ret = 2 * np.arctan2(np.sqrt(temp), np.sqrt(1-temp))
    return ret

def groupby_statistics(df):
    return df.groupby(by='Passenger_Count').agg(
      {
        'Fare_Amt': ['mean', 'std'],
        'Tip_Amt': ['mean', 'std']
      }
    )

def join_count(df, other):
    return len(df.merge(other, left_index=True, right_index=True))

def join_data(df, other):
    return df.merge(other, left_index=True, right_index=True)