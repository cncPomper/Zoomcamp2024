## Question 1. answer

```bash
pip show wheel
```

## Question 2. answer

```bash
pip show wheel
```

## Question 3. answer

```sql
SELECT
	*
FROM
	green_tripdata
WHERE
	CAST(lpep_pickup_datetime AS DATE) = '2019-09-18' AND
	CAST(lpep_dropoff_datetime AS DATE) = '2019-09-18'
```

## Question 4. answer

```sql
SELECT
	CAST(lpep_pickup_datetime AS DATE) as "lpd_day",
	MAX(trip_distance)
FROM
	green_tripdata
WHERE
	CAST(lpep_pickup_datetime AS DATE) = CAST(lpep_dropoff_datetime AS DATE)
GROUP BY
	CAST(lpep_pickup_datetime AS DATE)
ORDER BY MAX(trip_distance) DESC;
```

## Question 5. answer

```sql
SELECT
	*
FROM
	(SELECT
		zpu."Borough",
		SUM(total_amount) as sum_total_amount
	FROM
		green_tripdata g,
		zones zpu
	WHERE
		g."PULocationID" = zpu."LocationID" AND
		CAST(g.lpep_pickup_datetime as DATE) = '2019-09-18'
	GROUP BY
		zpu."Borough"
	ORDER BY SUM(total_amount) DESC) as q
WHERE
	q."sum_total_amount" >= 50000
```

## Question 6. answer

```sql
SELECT
	g."DOLocationID",
	MAX(g."tip_amount")
FROM
	green_tripdata g JOIN zones zpu
	ON g."PULocationID" = zpu."LocationID"
where
	CAST(g."lpep_pickup_datetime" as DATE) between '2019-09-01' AND '2019-09-30'
	AND zpu."Zone" = 'Astoria'
GROUP BY
	g."DOLocationID"
ORDER BY MAX(g."tip_amount") DESC
```

Since the top record to the upper query was '132' lets look up zones table

```sql
SELECT
	*
FROM zones
where zones."LocationID" = 132
```

## Question 7. answer

```bash
Do you want to perform these actions?
  Terraform will perform the actions described above.
  Only 'yes' will be accepted to approve.

  Enter a value: yes

google_bigquery_dataset.demo_dataset: Creating...
google_storage_bucket.demo-bucket: Creating...
google_storage_bucket.demo-bucket: Creation complete after 0s [id=hw1-terra-bucket]
google_bigquery_dataset.demo_dataset: Creation complete after 0s [id=projects/precise-rite-412717/datasets/hw1_dataset]

Apply complete! Resources: 2 added, 0 changed, 0 destroyed.
```

