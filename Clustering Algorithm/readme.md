# Clustering Algorithm
=================================================================================

## Abstract
This project is the homework 5 for USC DSCI 553 Data Mining.
In this project, you are going to implement three streaming algorithms. In the first two tasks, you
will generate a simulated data stream with the Yelp dataset and implement Bloom Filtering and
Flajolet-Martin algorithm. In the third task, you will do some analysis using Fixed Size Sample (Reservoir
Sampling).
Please see more details in the instruction.pdf.


## Data
Please download 'users.txt' and 'blackbox.py' from 'data.zip' file. 
For this project, you need to download the users.txt as the input file. You also need a Python
blackbox file to generate data from the input file. We use this blackbox as a simulation of a data stream.
The blackbox will return a list of User ids which are from file users.txt every time you ask it. It is possible
but very unlikely that the user ids returned might not be unique. You are required to handle this case
wherever required. Please call the function as this:

```console
from blackbox import BlackBox
bx = BlackBox()
stream_users = bx.ask(input_file,stream_size)
```

While doing the tasks, you might need to ask the blackbox multiple times. You may do it by the following
sample code:

```console
for _ in range(num_of_asks):
  stream_users = bx.ask(input_file,stream_size)
  your_function(stream_users)
```


Download the dataset 'data.zip' file and unzip it. We have generated a sub-dataset, 'ub_sample_data.csv', from the Yelp review dataset containing user_id
and business_id.


## Environment
- Python 3.6
- JDK 1.8
- Scala 2.11
- Spark 2.4.4

## Execution
Please pub the 'task*.py' file in the same directory with 'blackbox.py' file.
If the code is executed in local environment, simply run:

```console
python task1.py <input_filename> stream_size num_of_asks <output_filename>

python task2.py <input_filename> stream_size num_of_asks <output_filename>

python task3.py <input_filename> stream_size num_of_asks <output_filename>
```
