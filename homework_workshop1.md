# Homework - Workshop 1 (data load tool)

#### Question 1: What is the sum of the outputs of the generator for limit = 5?
- **A**: 10.23433234744176 
- **B**: 7.892332347441762
- **C**: 8.382332347441762 ✔
- **D**: 9.123332347441762

```
def square_root_generator(limit):
    n = 1
    while n <= limit:
        yield n ** 0.5
        n += 1

# Example usage:
limit = 5
generator = square_root_generator(limit)

result_sum = 0
for sqrt_value in generator:
    result_sum += sqrt_value
    
print(result_sum)
```

#### Question 2: What is the 13th number yielded by the generator?
- **A**: 4.236551275463989
- **B**: 3.605551275463989 ✔
- **C**: 2.345551275463989
- **D**: 5.678551275463989

```
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

#### Question 3: Append the 2 generators. After correctly appending the data, calculate the sum of all ages of people.
- **A**: 353 ✔
- **B**: 365
- **C**: 378
- **D**: 390

```
# Install packages.
import duckdb

# Create a connection and cursor. ':memory:' to indicate that the database should be created in memory or in this block alone.
con = duckdb.connect(database=':memory:', read_only=False)
cur = con.cursor()

# Create a table for people.
cur.execute("""
    CREATE TABLE people (
        ID INTEGER,
        Name VARCHAR,
        Age INTEGER,
        City VARCHAR,
        Occupation VARCHAR
    ) 
""")

# Load data from the first generator into the table
insert_query = "INSERT INTO people VALUES (?, ?, ?, ?, ?)" 
for person in people_1():
    cur.execute(insert_query, (person["ID"], person["Name"], person["Age"], person["City"], None)) 

# Calculate the sum of ages for the first generator
sum_query_1 = "SELECT SUM(Age) FROM people"
sum_1 = cur.execute(sum_query_1).fetchone()[0] #  Fetches the sum of ages from the first generator and storing it in the variable which is 'sum_1'
print("Sum of ages from the first generator:", sum_1)

# Append data from the second generator into the same table
for person in people_2():
    cur.execute(insert_query, (person["ID"], person["Name"], person["Age"], person["City"], person["Occupation"]))

# Calculate the sum of ages for both generators
sum_query_2 = "SELECT SUM(Age) FROM people"
sum_2 = cur.execute(sum_query_2).fetchone()[0] #  Fetches the sum of ages from the first generator and storing it in the variable which is 'sum_2'
print("Sum of ages from both generators after appending:", sum_2)

# Close the connection.
con.close()
```

#### Question 4: Merge the 2 generators using the ID column. Calculate the sum of ages of all the people loaded as described above.
- **A**: 205
- **B**: 213
- **C**: 221
- **D**: 230

``` 
#Install the dependencies
%%capture
!pip install dlt[duckdb]
```

```
# Define the connection to load to.
pipeline = dlt.pipeline(destination='duckdb', dataset_name='people')

info = pipeline.run(people_1(),
                    people_2(),
										table_name="rides",
										write_disposition="merge",
                    primary_key="ID")

print(info)

# Create a connection and cursor. ':memory:' to indicate that the database should be created in memory or in this block alone.
con = duckdb.connect(database=':memory:', read_only=False)
cur = con.cursor()

# Create a table for people.
cur.execute("""
    CREATE TABLE people (
        ID INTEGER PRIMARY KEY,
        Name VARCHAR,
        Age INTEGER,
        City VARCHAR,
        Occupation VARCHAR
    ) 
""")

# Load data from the first generator into the table
insert_query = "INSERT INTO people VALUES (?, ?, ?, ?, ?)" 
for person in people_1():
    cur.execute(insert_query, (person["ID"], person["Name"], person["Age"], person["City"], None)) 

# Calculate the sum of ages for the first generator
sum_query_1 = "SELECT SUM(Age) FROM people"
sum_1 = cur.execute(sum_query_1).fetchone()[0] #  Fetches the sum of ages from the first generator and storing it in the variable which is 'sum_1'
print("Sum of ages from the first generator:", sum_1)

# Append data from the second generator into the same table
for person in people_2():
    cur.execute(insert_query, (person["ID"], person["Name"], person["Age"], person["City"], person["Occupation"]))

# Calculate the sum of ages for both generators
sum_query_2 = "SELECT SUM(Age) FROM people"
sum_2 = cur.execute(sum_query_2).fetchone()[0] #  Fetches the sum of ages from the first generator and storing it in the variable which is 'sum_2'
print("Sum of ages from both generators after appending:", sum_2)

# Close the connection.
con.close()
```
