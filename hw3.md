## Question 1. answer

```sql
SELECT count(*)
FROM precise-rite-412717.hw3_dataset.green_taxi_2022_v2;
```

## Question 2. answer

## Question 3. answer

```sql
SELECT count(*)
FROM precise-rite-412717.hw3_dataset.green_taxi_2022_v2
WHERE fare_amount = 0;
```

## Question 4. answer

I was able only to create a table Partition by lpep_pickup_datetime Cluster on PUlocationID

## Question 5. answer

```sql
-- NO Partition (12.82MB)
SELECT DISTINCT(PUlocationID)
FROM precise-rite-412717.hw3_dataset.green_taxi_2022_v2
WHERE lpep_pickup_datetime BETWEEN '2022-06-01' AND '2022-06-30';

-- Partition (1.12MB)
SELECT DISTINCT(PUlocationID)
FROM precise-rite-412717.hw3_dataset.green_taxi_2022_hw3_table_partitioned_clustered
WHERE lpep_pickup_datetime BETWEEN '2022-06-01' AND '2022-06-30';
```

## Question 6. answer

Lets look in the GCP Bucket

## Question 7. answer

If there is a lot of unique values then it will be inefficent to do so, since there is a limit of partitions of 400.