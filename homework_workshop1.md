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
- **A**: 215
- **B**: 266 ✔
- **C**: 241
- **D**: 258

``` 
#Install the dependencies
%%capture
!pip install dlt[duckdb]
```

```
import duckdb
import dlt

# Initialize empty lists to store data.
data = []
data2 = []

# Load first generator into data.
def people_1():
    for i in range(1, 6):
        data.append({"ID": i, "Name": f"Person_{i}", "Age": 25 + i, "City": "City_A"})

# Merge the second generator.
def people_2():
    for i in range(3, 9):
        data2.append({"ID": i, "Name": f"Person_{i}", "Age": 30 + i, "City": "City_B", "Occupation": f"Job_{i}"})

# Call the generator functions to populate the data lists.
people_1()
people_2()

# Load the first generator.
pipeline = dlt.pipeline(destination='duckdb', dataset_name='humans_dataset')

# Create primary for the first generator.
info = pipeline.run(data,
										table_name="humans",
										write_disposition="merge",
                    primary_key="ID")

# Merge data2 with the existing data.
pipeline = dlt.pipeline(destination='duckdb', dataset_name='humans_dataset')

# Run the pipeline.
info = pipeline.run(data2,
										table_name="humans",
										write_disposition="merge",
                    primary_key='ID')

# Show the outcome.
conn = duckdb.connect(f"{pipeline.pipeline_name}.duckdb")

# Set the tables.
conn.sql(f"SET search_path = '{pipeline.dataset_name}'")
print('Loaded tables: ')
display(conn.sql("show tables"))

# Display the sum of ages from the "humans" table.
rides = conn.sql("SELECT SUM(age) FROM humans").df()
display(rides)
```
