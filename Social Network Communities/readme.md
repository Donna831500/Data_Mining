# Social Network Communities
=================================================================================

## Abstract
This project is the homework 4 for USC DSCI 553 Data Mining.
In this project, you will explore the spark GraphFrames library as well as implement your own GirvanNewman algorithm using the Spark Framework to detect communities in graphs. You will use the
ub_sample_data.csv dataset to find users who have a similar business taste. The goal of this project
is to help you understand how to use the Girvan-Newman algorithm to detect communities in an efficient
way within a distributed environment.
Please see more details in the instruction.pdf.


## Data
Download the dataset 'data.zip' file and unzip it. We have generated a sub-dataset, 'ub_sample_data.csv', from the Yelp review dataset containing user_id
and business_id.


## Environment
- Python 3.6
- JDK 1.8
- Scala 2.11
- Spark 2.4.4

## Execution
If the code is executed in local environment, simply run:

```console
python task1.py <filter threshold> <input_file_path> <community_output_file_path>

python task2.py <filter threshold> <input_file_path> <betweenness_output_file_path> <community_output_file_path>
```

If the code is executed in Vocareum, simply run:

```console
spark-submit --packages graphframes:graphframes:0.6.0-spark2.3-s_2.11 task1.py <filter threshold> <input_file_path> <community_output_file_path>

spark-submit task2.py <filter threshold> <input_file_path> <betweenness_output_file_path> <community_output_file_path>
```

Input parameters:
1. <filter threshold>: the filter threshold to generate edges between user nodes.
2. <input file path>: the path to the input file including path, file name and extension.
3. <betweenness output file path>: the path to the betweenness output file including path, file name
and extension.
4. <community output file path>: the path to the community output file including path, file name and
extension.
