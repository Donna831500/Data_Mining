# Recommendation Systems
=================================================================================

## Abstract
This project is the homework 3 for USC DSCI 553 Data Mining.
The goal is to familiarize you with Locality Sensitive
Hashing (LSH), and different types of collaborative-filtering recommendation systems.

## Data
In this assignment, the datasets you are going to use are from:
https://drive.google.com/drive/folders/1SufecRrgj1yWMOVdERmBBUnqz0EX7ARQ?usp=sharing
Or download the dataset 'data.zip' file and unzip it.
We generated the following two datasets from the original Yelp review dataset with some filters. We
randomly took 60% of the data as the training dataset, 20% of the data as the validation dataset, and
20% of the data as the testing dataset.

a. yelp_train.csv: the training data, which only include the columns: user_id, business_id, and stars.
b. yelp_val.csv: the validation data, which are in the same format as training data.
c. We are not sharing the test dataset.
d. other datasets: providing additional information (like the average star or location of a business)


## Environment
- Python 3.6
- JDK 1.8
- Scala 2.11
- Spark 2.4.4

## Execution
If the code is executed in local environment, simply run:

```console
python task1.py <input_file_name> <output_file_name>

python task2_1.py <train_file_name> <test_file_name> <output_file_name>

python task2_2.py <folder_path> <test_file_name> <output_file_name>

python task2_3.py <folder_path> <test_file_name> <output_file_name>
```

If the code is executed in Vocareum, simply run:

```console
spark-submit task1.py <input_file_name> <output_file_name>

spark-submit task2_1.py <train_file_name> <test_file_name> <output_file_name>

spark-submit task2_2.py <folder_path> <test_file_name> <output_file_name>

spark-submit task2_3.py <folder_path> <test_file_name> <output_file_name>
```
