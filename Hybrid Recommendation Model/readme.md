# Hybrid Recommendation Model
=================================================================================

## Abstract
This final project for USC DSCI 553 Data Mining course.
In this project, you need to significantly improve the performance of your recommendation
system in Assignment 3 (Recommendation Systems). You can use any method (like the hybrid recommendation systems) to improve
the prediction accuracy and efficiency.
Please see more details in the instruction.pdf.


## Data
In this project, the datasets you are going to use are from:
https://drive.google.com/drive/folders/1SIlY40owpVcGXJw3xeXk76afCwtSUx11?usp=sharing

We generated the following two datasets from the original Yelp review dataset with some filters. We
randomly took 60% of the data as the training dataset, 20% of the data as the validation dataset, and 20%
of the data as the testing dataset.
A. yelp_train.csv: the training data, which only include the columns: user_id, business_id, and stars.
B. yelp_val.csv: the validation data, which are in the same format as training data.
C. We are not sharing the test dataset.
D. other datasets: providing additional information (like the average star or location of a business)
 a. review_train.json: review data only for the training pairs (user, business)
 b. user.json: all user metadata
 c. business.json: all business metadata, including locations, attributes, and categories
 d. checkin.json: user check-ins for individual businesses
 e. tip.json: tips (short reviews) written by a user about a business
 f. photo.json: photo data, including captions and classifications


## Environment
- Python 3.6
- JDK 1.8
- Scala 2.11
- Spark 2.4.4

## Execution
If the code is executed in local environment, simply run:

```console
python competition.py <folder_path> <test_file_name> <output_file_name>
```

If the code is executed in Vocareum, simply run:

```console
spark-submit competition.py <folder_path> <test_file_name> <output_file_name>
```

Input parameters:
1. <folder_path>: the path of dataset folder, which contains exactly the same file as the google drive
2. <test_file_name>: the name of the testing file (e.g., yelp_val.csv), including the file path
3. <output_file_name>: the name of the prediction result file, including the file path
