import pandas as pd
import joblib
import pyarrow.parquet as pq
import os
import numpy as np

def reduce_and_save_parquet_in_chunks(input_file: str, output_file: str, chunk_size: int = 100000, sample_ratio: float = 1/24):
    """
    Reduce the size of a large Parquet file by processing it in chunks and sampling rows.

    Args:
        input_file: Path to the input Parquet file
        output_file: Path to save the reduced Parquet file
        chunk_size: Number of rows to process at a time
        sample_ratio: Fraction of rows to keep (e.g., 1/8 means keep 12.5% of rows)
    """
    parquet_file = pq.ParquetFile(input_file)
    total_rows = parquet_file.metadata.num_rows

    print(f"Total rows in the Parquet file: {total_rows}")
    print(f"Processing in chunks of {chunk_size} rows")
    os.makedirs(os.path.dirname(output_file) or '.', exist_ok=True)

    num_rows_to_sample = int(total_rows * sample_ratio)
    sampled_indices = set(np.random.choice(total_rows, size=num_rows_to_sample, replace=False))

    # Process the file in chunks
    row_count = 0
    sampled_dfs = []

    for i, batch in enumerate(parquet_file.iter_batches(batch_size=chunk_size)):
        df_chunk = batch.to_pandas()
        chunk_start_idx = row_count

        # Select rows that are in our sampled indices
        mask = [chunk_start_idx + j in sampled_indices for j in range(len(df_chunk))]
        sampled_chunk = df_chunk.loc[mask]

        if not sampled_chunk.empty:
            sampled_dfs.append(sampled_chunk)

        row_count += len(df_chunk)
        print(f"Processed chunk {i+1}, rows {chunk_start_idx} to {row_count}")

    # Combine all sampled chunks
    if sampled_dfs:
        df_reduced = pd.concat(sampled_dfs, ignore_index=True)
        print(f"Final reduced dataset has {len(df_reduced)} rows")

        # Save the reduced DataFrame to Parquet
        df_reduced.to_parquet(output_file, index=False)
        print(f"Reduced dataset saved to {output_file}")
    else:
        print("No rows were sampled. Check your sampling ratio.")


if __name__ == "__main__":
    input_file = '../../datasets/yellow_tripdata_2009-02.parquet'
    output_file = '../../datasets/yellow_tripdata_2009-01_reduced.parquet'
    reduce_and_save_parquet_in_chunks(input_file, output_file)
