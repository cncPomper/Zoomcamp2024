# Question 1

```python
def square_root_generator(limit):
    n = 1
    while n <= limit:
        yield n ** 0.5
        n += 1

limit = 5
generator = square_root_generator(limit)

gen_sum = 0

for sqrt_value in generator:
    gen_sum += sqrt_value

print(gen_sum)
```

```
Output: 8.382
```

# Question 2

```python
def square_root_generator(limit):
    n = 1
    while n <= limit:
        yield n ** 0.5
        n += 1

# Example usage:
limit = 13
generator = square_root_generator(limit)

for sqrt_value in generator:
    print(sqrt_value)
```

```
Output: 3.605
```

# Question 3

```python

def people_1():
    for i in range(1, 6):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 25 + i, "City": "City_A"}

def people_2():
    for i in range(3, 9):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 30 + i, "City": "City_B", "Occupation": f"Job_{i}"}

pip install dlt[duckdb]

import dlt

# define the connection to load to.
# We now use duckdb, but you can switch to Bigquery later
generators_pipeline = dlt.pipeline(destination='duckdb', dataset_name='generators')


# we can load any generator to a table at the pipeline destnation as follows:
info = generators_pipeline.run(people_1(),
                            table_name="people",
                            write_disposition="replace")


info = generators_pipeline.run(people_2(),
                        table_name="people",
                        write_disposition="append")

import duckdb

conn = duckdb.connect(f"{generators_pipeline.pipeline_name}.duckdb")

conn.sql(f"SET search_path = '{generators_pipeline.dataset_name}'")
print('Loaded tables: ')
display(conn.sql("show tables"))


print("\n\n\n http_download table below:")

people = conn.sql("SELECT * FROM people").df()
display(people)


people = conn.sql("SELECT sum(age) FROM people").df()
display(people)
```

```
Output: 353
```


# Question 4

```python

def people_1():
    for i in range(1, 6):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 25 + i, "City": "City_A"}

def people_2():
    for i in range(3, 9):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 30 + i, "City": "City_B", "Occupation": f"Job_{i}"}

pip install dlt[duckdb]

import dlt

# define the connection to load to.
# We now use duckdb, but you can switch to Bigquery later
generators_pipeline = dlt.pipeline(destination='duckdb', dataset_name='generators')


# we can load any generator to a table at the pipeline destnation as follows:
# we can load any generator to a table at the pipeline destnation as follows:
info = generators_pipeline.run(people_1(),
                                    table_name="people",
                                    write_disposition="replace",
                                    primary_key="id")

# the outcome metadata is returned by the load and we can inspect it by printing it.
print(info)

# we can load the next generator to the same or to a different table.
info = generators_pipeline.run(people_2(),
                                    table_name="people",
                                    write_disposition="merge",
                                    primary_key="id")

# show outcome

import duckdb

conn = duckdb.connect(f"{generators_pipeline.pipeline_name}.duckdb")

# let's see the tables
conn.sql(f"SET search_path = '{generators_pipeline.dataset_name}'")
print('Loaded tables: ')
display(conn.sql("show tables"))

print("\n\n\n http_download table below:")

people = conn.sql("SELECT sum(age) FROM people").df()
display(people)
```

```
Output: 266
```