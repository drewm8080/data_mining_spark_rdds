# Project Summary

## Task 1: Data Exploration
- Utilized test_review.json to extract various information:
  - Total number of reviews
  - Number of reviews in 2018
  - Number of distinct users who wrote reviews
  - Top 10 users who wrote the largest numbers of reviews and the number of reviews they wrote
  - Number of distinct businesses that have been reviewed
  - Top 10 businesses that had the largest numbers of reviews and the number of reviews they had
- Output the results in a JSON format file.

## Task 2: Partition
- Showed the number of partitions for the RDD used for Task 1 Question F and the number of items per partition.
- Used a customized partition function to improve the performance of map and reduce tasks.
- Compared the time duration between the system default partition and the customized partition.

## Task 3: Exploration on Multiple Datasets
- Explored review information (test_review.json) and business information (business.json) together.
- Answered questions such as the average stars for each city.
- Compared the execution time of using two methods to print the top 10 cities with the highest average stars.

The code was executed using the provided input format and the results were written in the specified output format. The project ensured that the code works well on large datasets, as required.

## Final Results
- **Grade**: 100%
