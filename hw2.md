## Question 1. answer

```python
def load_data_from_api(*args, **kwargs):

    taxi_dtypes = {
        'VendorID' : pd.Int64Dtype(),
        'passenger_count': pd.Int64Dtype(),
        'trip_distance': float,
        'RatecodeID': pd.Int64Dtype(),
        'store_and_fwd_flag': str,
        'PULocationID': pd.Int64Dtype(),
        'DOLocationID': pd.Int64Dtype(),
        'payment_type': pd.Int64Dtype(),
        'fare_amount': float,
        'extra': float,
        'mta_tax': float,
        'tip_amount': float,
        'tolls_amount': float,
        'improvement_surcharge': float,
        'total_amount': float,
        'congestion_surcharge': float
    }

    parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']

    url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-10.csv.gz"

    df = pd.read_csv(url,
                        sep=",",
                        compression="gzip",
                        dtype=taxi_dtypes,
                        parse_dates=parse_dates)

    for month in ["11", "12"]:

        url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-{month}.csv.gz"

        df_foo = pd.read_csv(url,
                            sep=",",
                            compression="gzip",
                            dtype=taxi_dtypes,
                            parse_dates=parse_dates)

        df = pd.concat([df, df_foo], ignore_index=True)

    return df
```

To find dataset shape we run
```python
return df.shape # Instead of `return df`
```

# Transform code for the questions 2-5

```python
def transform(data, *args, **kwargs):

    print(f"Preprocessing: rows with zero passengers: {data['passenger_count'].isin([0]).sum()}")
    print(f"Preprocessing: rows with trip_distance equal to zero: {data['trip_distance'].isin([0]).sum()}")

    data = data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]

    # lpep to datetime
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date
    data['lpep_dropoff_date'] = data['lpep_dropoff_datetime'].dt.date

    # Camel Case to Snake Case
    data.columns = (data.columns.str.replace('ID', '_id')
                                .str.replace(' ', '_')
                                .str.lower()
                    )
    return data
```

## Question 2. answer

```python
data = data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]

len(data)
```

## Question 3. answer

```python
data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date
```

## Question 4. answer

```python
set(data.vendor_id)
```

Output:
```python
[1, 2]
```

## Question 5. answer

By hand finding which column names do change
```python
data.columns
```

## Question 6. answer

### Task 1

```python
def export_data_to_postgres(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a PostgreSQL database.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#postgresql
    """
    schema_name = 'mage'  # Specify the name of the schema to export data to
    table_name = 'green_taxi'  # Specify the name of the table to export data to
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'dev'

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        loader.export(
            df,
            schema_name,
            table_name,
            index=False,  # Specifies whether to include index in exported table
            if_exists='replace',  # Specify resolution policy if table name already exists
        )
```

To find if we successfully uploaded the data to db:

```sql
SELECT * FROM mage.green_taxi LIMIT 10;
```

### Task 2

```python
import pyarrow as pa
import pyarrow.parquet as pq
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/precise-rite-412717-a60ab33c96ef.json"
bucket_name = 'mage-zoomcamp-week2-piotr-michal-kitlowski-24'
project_id = 'precise-rite-412717'

table_name = 'green_taxi'

root_path = f"{bucket_name}/{table_name}"

@data_exporter
def export_data(data, *args, **kwargs):
    # data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    table = pa.Table.from_pandas(data)

    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=['lpep_pickup_date'],
        filesystem=gcs
    )
```