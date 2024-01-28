import pandas as pd
from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector
from pathlib import Path
from prefect_gcp.cloud_storage import GcsBucket


def extract(dataset_url: str, start_month: int, end_month: int) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame for the specified months"""
    df_list = []

    for month in range(start_month, end_month + 1):
        month_str = str(month).zfill(2)
        url = f"{dataset_url}-{month_str}.csv.gz"
        print(f"Attempting to read data from: {url}")

        try:
            df_month = pd.read_csv(url, low_memory=False)
            df_list.append(df_month)
            print(f"Successfully loaded data for {month_str} with {len(df_month)} rows")
        except Exception as e:
            print(f"Error loading data for {month_str}: {e}")

    final_df = pd.concat(df_list, ignore_index=True)
    print(f"Successfully loaded data for {start_month}-{end_month} with {len(final_df)} rows")
    return final_df


@task(log_prints=True)
def transform(df: pd.DataFrame):
    # Convert passenger_count to bigint and handle non-integer values
    df['passenger_count'] = pd.to_numeric(df['passenger_count'], errors='coerce')

    pre_condition_count = ((df['passenger_count'] == 0) | (df['trip_distance'] == 0)).sum()
    print(f"pre: rows with passenger count=0 or trip distance=0: {pre_condition_count}")

    # Filter rows where passenger count is not equal to 0 and trip distance is not equal to 0
    df = df[(df['passenger_count'] != 0) | (df['trip_distance'] != 0)]

    post_condition_count = ((df['passenger_count'] == 0) | (df['trip_distance'] == 0)).sum()
    print(f"post: rows with passenger count=0 or trip distance=0: {post_condition_count}")

    # Add a new column 'lpep_pickup_date' by converting 'lpep_pickup_datetime' to a date
    # df['lpep_pickup_date'] = pd.to_datetime(df['lpep_pickup_datetime']).dt.date
    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    df['lpep_pickup_date'] = df['lpep_pickup_datetime'].dt.date

    print(f"Transformed DataFrame with {len(df)} rows")

    # Assuming df is your DataFrame
    column_names = df.columns

    # Count columns not in snake case
    not_snake_case_count = sum(1 for col in column_names if not col.isidentifier() or not col.islower())

    print(f"Number of columns not in snake case: {not_snake_case_count}")

    return df


@task(log_prints=True, retries=3)
def load_data(table_name, df, expected_rows):
    connection_block = SqlAlchemyConnector.load("postgres-connector")
    with connection_block.get_connection(begin=False) as engine:
        if len(df) != expected_rows:
            raise ValueError(f"Expected {expected_rows} rows, but got {len(df)} rows.")

        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
        df.to_sql(name=table_name, con=engine, if_exists='append')


@flow(name="Subflow", log_prints=True)
def log_subflow(table_name: str, rows_count: int):
    print(f"Logging Subflow for: {table_name} with {rows_count} rows")


@flow(name="Ingest Data")
def main_flow(table_name: str = "green_taxi", start_month: int = 10, end_month: int = 12):
    color = "green"
    year = 2020
    dataset_file = f"{color}_tripdata_{year}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}"

    raw_data = extract(dataset_url, start_month, end_month)
    data = transform(raw_data)

    log_subflow(table_name, len(data))

    expected_rows = len(data)
    load_data(table_name, data, expected_rows)


if __name__ == '__main__':
    main_flow(table_name="green_taxi")
