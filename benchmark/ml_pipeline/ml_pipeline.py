import os

from dask.distributed import Client
from xgboost.dask import DaskXGBRegressor
from dotenv import load_dotenv
import dask.dataframe as dd
import matplotlib.pyplot as plt
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import numpy as np
import json
import pandas as pd
import time
import logging
from dask_ml.model_selection import train_test_split 

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def main():
    start_total = time.time()
    
    logging.info("Pipeline started")
    
    load_dotenv()

    workers = os.getenv("DASK_WORKERS")
    memory_limit = os.getenv("DASK_MEMORY_LIMIT")

    client = Client(
        n_workers=8,
        memory_limit="40GB",
        threads_per_worker=4,
        processes=True
    )
    logging.info("Client set") 

    bucket_path = os.getenv("GCP_BUCKET_PATH")

    file_names = [f"{bucket_path}/yellow_tripdata_2011-{str(month).zfill(2)}.parquet"
              for month in range(1, 13)]

    df_final = None

    for file in file_names: 
        df = dd.read_parquet(file, storage_options={"token": "cloud"})  

        start_prep = time.time()
        
        df = df.drop(['store_and_fwd_flag', 'congestion_surcharge', 'airport_fee',
                            'mta_tax', 'improvement_surcharge', 'tolls_amount'], axis=1)
    
        df = df[(df["passenger_count"] != 0) & (df["trip_distance"] != 0)]
    
        df["tpep_pickup_datetime"] = dd.to_datetime(df["tpep_pickup_datetime"])
        df["hour_pickup"] = df["tpep_pickup_datetime"].dt.hour
        df["day_of_the_week_pickup"] = df["tpep_pickup_datetime"].dt.dayofweek
        df["month_pickup"] = df["tpep_pickup_datetime"].dt.month
    
        df["tpep_dropoff_datetime"] = dd.to_datetime(df["tpep_dropoff_datetime"])
        df["hour_dropoff"] = df["tpep_dropoff_datetime"].dt.hour
        df["day_of_the_week_dropoff"] = df["tpep_dropoff_datetime"].dt.dayofweek
        df["month_dropoff"] = df["tpep_dropoff_datetime"].dt.month
    
        df = df.drop(['tpep_pickup_datetime', 'tpep_dropoff_datetime'], axis=1)

        if df_final is None:
            df_final = df
        else:
            df_final = dd.concat([df_final, df], ignore_index=True)

        del df
        logging.info(f"Time to preprocess df: {time.time() - start_prep:.2f} segs")
        

    X = df_final.drop("fare_amount", axis=1)
    y = df_final["fare_amount"]
    del df_final

    X_train, X_test, y_train_reg, y_test_reg = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    del X, y

    X_train_dask = X_train.to_dask_array(lengths=True)
    y_train_dask = y_train_reg.to_dask_array(lengths=True)
    X_test_dask = X_test.to_dask_array(lengths=True)

    logging.info("Split done")
    
    start_train = time.time()

    param_grid = {
        "n_estimators": [50, 100, 200],
        "learning_rate": [0.01, 0.1, 0.2],
        "max_depth": [3, 5, 7]
    }

    best_model = None
    best_score = float("inf")
    best_params = None

    for n in param_grid["n_estimators"]:
        for lr in param_grid["learning_rate"]:
            for md in param_grid["max_depth"]:
                logging.info(f"Treinando com: n_estimators={n}, learning_rate={lr}, max_depth={md}")
                model = DaskXGBRegressor(
                    n_estimators=n,
                    learning_rate=lr,
                    max_depth=md,
                    tree_method="hist",
                    verbosity=0
                )
                model.client = client
                model.fit(X_train_dask, y_train_dask)

                y_pred_test = model.predict(X_test_dask).compute()
                rmse = np.sqrt(mean_squared_error(y_test_reg, y_pred_test))

                logging.info(f"RMSE: {rmse:.4f}")

                if rmse < best_score:
                    best_model = model
                    best_score = rmse
                    best_params = {
                        "n_estimators": n,
                        "learning_rate": lr,
                        "max_depth": md
                    }

    logging.info(f"Time to train model: {time.time() - start_train:.2f} segs")

    y_pred = best_model.predict(X_test_dask).compute()
    y_test_np = y_test_reg.to_dask_array(lengths=True).compute()

    rmse = np.sqrt(mean_squared_error(y_test_np, y_pred))
    mae = mean_absolute_error(y_test_np, y_pred)
    r2 = r2_score(y_test_np, y_pred)

    logging.info(f"Final Scores - RMSE: {rmse:.2f}, MAE: {mae:.2f}, R²: {r2:.4f}")

    residuals = y_test_np - y_pred
    plt.figure(figsize=(10, 6))
    plt.hist(residuals, bins=50)
    plt.title("Distribuição dos Resíduos (Erro de Previsão)")
    plt.xlabel("Erro")
    plt.ylabel("Frequência")
    plt.grid(True)
    plt.xlim(-5, 5) 
    plt.tight_layout()
    plt.savefig("residuals_plot.png")
    plt.close() 

    importances = best_model.feature_importances_
    features = X_train.columns
    
    importance_df = pd.DataFrame({
        "Feature": features,
        "Importance": importances
    }).sort_values(by="Importance", ascending=False)

    plt.figure(figsize=(10, 6))
    plt.barh(importance_df["Feature"], importance_df["Importance"])
    plt.gca().invert_yaxis()
    plt.title("Importância das Features (XGBoost)")
    plt.xlabel("Importância")
    plt.tight_layout()
    plt.savefig("feature_importance_plot.png")
    plt.close() 

    metrics = {
        "RMSE": round(rmse, 3),
        "MAE": round(mae, 3),
        "R2": round(r2, 4),
        "Best_Params": best_params
    }
    with open("evaluation_metrics.json", 'w') as f_metrics:
        json.dump(metrics, f_metrics)

    importance_df.to_csv("feature_importance.csv", index=False)

    logging.info(f"Total time to run: {time.time() - start_total:.2f} segs")

if __name__ == "__main__":
    main()
