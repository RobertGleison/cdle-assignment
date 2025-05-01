import pyspark.pandas as ks
import numpy as np


def read_file_parquet(df=None, **kwargs):
    return ks.read_parquet(kwargs.get("path"))

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
    x = df.Fare_Amt + df.Tip_Amt
    return x

def mean_of_product(df):
    return (df.Fare_Amt * df.Tip_Amt).mean()

def product_columns(df):
    x = df.Fare_Amt * df.Tip_Amt
    return x

def value_counts(df):
    val_counts = df.Fare_Amt.value_counts()
    return val_counts

def complicated_arithmetic_operation(df):
     # Use numpy functions for trigonometric operations
    theta_1 = np.radians(df['Start_Lon'])  # Convert to radians
    phi_1 = np.radians(df['Start_Lat'])    # Convert to radians
    theta_2 = np.radians(df['End_Lon'])    # Convert to radians
    phi_2 = np.radians(df['End_Lat'])      # Convert to radians

    # Haversine formula to calculate the great-circle distance between two points on the Earth's surface
    temp = (np.sin((theta_2 - theta_1) / 2) ** 2 +
            np.cos(theta_1) * np.cos(theta_2) * np.sin((phi_2 - phi_1) / 2) ** 2)

    # Apply arctangent formula to get distance
    ret = np.arctan2(np.sqrt(temp), np.sqrt(1 - temp)) * 2

    return ret

def mean_of_complicated_arithmetic_operation(df):
     # Use numpy functions for trigonometric operations
    theta_1 = np.radians(df['Start_Lon'])  # Convert to radians
    phi_1 = np.radians(df['Start_Lat'])    # Convert to radians
    theta_2 = np.radians(df['End_Lon'])    # Convert to radians
    phi_2 = np.radians(df['End_Lat'])      # Convert to radians

    # Haversine formula to calculate the great-circle distance between two points on the Earth's surface
    temp = (np.sin((theta_2 - theta_1) / 2) ** 2 +
            np.cos(theta_1) * np.cos(theta_2) * np.sin((phi_2 - phi_1) / 2) ** 2)

    # Apply arctangent formula to get distance
    ret = np.arctan2(np.sqrt(temp), np.sqrt(1 - temp)) * 2

    return ret.mean()



def groupby_statistics(df):
    gb = df.groupby(by='Passenger_Count').agg(
      {
        'Fare_Amt': ['mean', 'std'],
        'Tip_Amt': ['mean', 'std']
      }
    )
    return gb

def join_count(df, other):
    joined = df.join(other, on="Passenger_Count")  # Join on the correct column
    return joined.count()

def join_data(df, other):
    return df.join(other, on="Passenger_Count")  # Join on the correct column