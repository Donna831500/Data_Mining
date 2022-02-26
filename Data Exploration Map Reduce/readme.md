# Data Exploration: Map Reduce
=================================================================================

## Abstract
This project is the homework 1 for USC DSCI 553 Data Mining.
The goal of these tasks is to get you familiar with Spark
operation types (e.g., transformations and actions) and explore a real-world dataset: Yelp dataset
(https://www.yelp.com/dataset).
Please see more details in the instruction.pdf.

## Data
Please download Yelp dataset from https://www.yelp.com/dataset
The two files business.json and test_review.json are the two files you will work on for this project.

## Environment
- Python 3.6
- JDK 1.8
- Scala 2.11
- Spark 2.4.4

## Execution
If the code is executed in local environment, simply run:

```console
python task1.py <review_filepath> <output_filepath>
python task2.py <review_filepath> <output_filepath> <n_partition>
python task3.py <review_filepath> <output_filepath> <output_filepath_question_a> <output_filepath_question_b>
```

If the code is executed in Vocareum, simply run:

spark-submit --executor-memory 4G --driver-memory 4G task1.py <review_filepath> <business_filepath>

spark-submit --executor-memory 4G --driver-memory 4G task2.py <review_filepath> <business_filepath> <n_partition>

spark-submit --executor-memory 4G --driver-memory 4G task3.py <review_filepath> <business_filepath> <output_filepath_question_a> <output_filepath_question_b>
