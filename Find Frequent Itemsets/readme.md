# Find Frequent Itemsets
=================================================================================

## Abstract
This project is the homework 2 for USC DSCI 553 Data Mining.
In this project, you will implement the SON Algorithm using the Spark Framework. You will develop
a program to find frequent itemsets in two datasets, one simulated dataset and one real-world
generated dataset. The goal of this project is to apply the algorithms you have learned in class on
large datasets more efficiently in a distributed environment.

## Data
In this project, you will use one simulated dataset and one real-world. In task 1, you will build and
test your program with a small simulated CSV file that has been provided to you.
Then in task2 you need to generate a subset using the Ta Feng dataset (https://bit.ly/2miWqFS) with a
structure similar to the simulated data. Or download the dataset 'data.zip' file and unzip it.


## Environment
- Python 3.6
- JDK 1.8
- Scala 2.11
- Spark 2.4.4

## Execution
If the code is executed in local environment, simply run:

```console
python task1.py <case number> <support> <input_file_path> <output_file_path>

python task2.py <filter threshold> <support> <input_file_path> <output_file_path>
```

If the code is executed in Vocareum, simply run:

spark-submit task1.py <case number> <support> <input_file_path> <output_file_path>

spark-submit task2.py <filter threshold> <support> <input_file_path> <output_file_path>
