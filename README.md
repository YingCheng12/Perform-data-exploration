# Perform-data-exploration

Task1: Data Exploration
You will explore the dataset, review.json, containing review information for this task, and you need to write a program to automatically answer the following questions:
A. The total number of reviews 
B. The number of reviews in 2018 
C. The number of distinct users who wrote reviews 
D. The top 10 users who wrote the largest numbers of reviews and the number of reviews they wrote 
E. The number of distinct businesses that have been reviewed 
F. The top 10 businesses that had the largest numbers of reviews and the number of reviews they had 


Task2: Partition (2 points)
Since processing large volumes of data requires performance decisions, properly partitioning the data for processing is imperative.
In this task, you will show the number of partitions for the RDD used for Task 1 Question F and the number of items per partition. 
Then, you need to use a customized partition function to improve the performance of map and reduce tasks. 
A time duration (for executing Task 1 Question F) comparison between the default partition and the customized partition (RDD built using the partition function) should also be shown in your results.


Task3: Exploration on Multiple Datasets 
In task3, you are asked to explore two datasets together containing review information (review.json) and business information (business.json) and write a program to answer the following questions:
A. What is the average stars for each city? (DO NOT use the stars information in the business file) (1 point) 
B. You are required to use two ways to print top 10 cities with highest stars. 
You need to compare the time difference between two methods and explain the result within 1 or 2 sentences. 
Method1: Collect all the data, and then print the first 10 cities
Method2: Take the first 10 cities, and then print all
