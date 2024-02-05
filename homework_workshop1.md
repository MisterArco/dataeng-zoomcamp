# Homework - Workshop 1 (data load tool)

#### Question 1: What is the sum of the outputs of the generator for limit = 5?
- **A**: 10.23433234744176
- **B**: 7.892332347441762
- **C**: 8.382332347441762
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
- **B**: 3.605551275463989
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
- **A**: 353
- **B**: 365
- **C**: 378
- **D**: 390

#### Question 4: Merge the 2 generators using the ID column. Calculate the sum of ages of all the people loaded as described above.
- **A**: 205
- **B**: 213
- **C**: 221
- **D**: 230
