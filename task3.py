from pyspark import SparkContext
import sys
import time
import json

def read_data(filepath_1,filepath_2):
    test_review_rdd = sc.textFile(filepath_1).map(lambda x: json.loads(x))
    business_rdd = sc.textFile(filepath_2).map(lambda x: json.loads(x))
    return business_rdd, test_review_rdd

def join_data(rdd1,rdd2):
    final_rdd = rdd1.leftOuterJoin(rdd2)
    return final_rdd

def average_rdd(joined_rdd):
    # [1][1] is the city, [1][0] is the count, then reduce by key so we get the final count 
    sum_count = joined_rdd.map(lambda x: (x[1][1], (x[1][0], 1))).reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    # sum and count
    average_rdd = sum_count.map(lambda x: (x[0], x[1][0] / x[1][1]))
    # sorting the averages 
    return average_rdd

if __name__ == '__main__':
    # Create a SparkContext object
    sc = SparkContext('local[*]', 'task3')
    input_filepath_1 =sys.argv[1]
    input_filepath_2 = sys.argv[2]
    output_filepath_1 = sys.argv[3]
    #"/Users/andrewmoore/Desktop/DSCI 553/DSCI 553 HW 1/output/output3.txt"
    output_filepath_2 = sys.argv[4]
    #"/Users/andrewmoore/Desktop/DSCI 553/DSCI 553 HW 1/output/output4.json"

    # part 3a
    # Reading in the data
    data = read_data(input_filepath_1,input_filepath_2)
    business_rdd = data[0]
    test_review_rdd = data[1]
    # mapping the data -- (stars,city)
    business_rdd = business_rdd.map(lambda x:(x['business_id'],x['city']))
    test_review_rdd = test_review_rdd.map(lambda x:(x['business_id'],x['stars']))

    #joining the data
    joined_rdd = join_data(test_review_rdd,business_rdd)
    
    # averaging the data
    avg_rdd = average_rdd(joined_rdd)

    #sorting the data
    sorted_average_rdd = avg_rdd.sortBy(lambda x: (-x[1], x[0]))

    
    
    
    #part 3b
    final_dictionary = {}
    # sorting using python
    python_time_start = time.time()
    # Reading in the data
    data = read_data(input_filepath_1,input_filepath_2)
    business_rdd = data[0]
    test_review_rdd = data[1]   
    # mapping the data -- (stars,city)
    business_rdd = business_rdd.map(lambda x:(x['business_id'],x['city']))
    test_review_rdd = test_review_rdd.map(lambda x:(x['business_id'],x['stars']))
    #joining the data
    joined_rdd = join_data(test_review_rdd,business_rdd)
    # averaging the data
    avg_rdd = average_rdd(joined_rdd).collect()
    # sort
    sorted_list = sorted(avg_rdd, key= lambda x: (-x[1], x[0]))
    ten_items = sorted_list[:10]
    top_10_items = [x[0] for x in ten_items]
    print(top_10_items)
    python_time_end = time.time()
    final_time_python= python_time_end-python_time_start
    final_dictionary['m1']= final_time_python


    # sorting using spark
    spark_time_start = time.time()
    # Reading in the data
    data = read_data(input_filepath_1,input_filepath_2)
    business_rdd = data[0]
    test_review_rdd = data[1]   
    # mapping the data -- (stars,city)
    business_rdd = business_rdd.map(lambda x:(x['business_id'],x['city']))
    test_review_rdd = test_review_rdd.map(lambda x:(x['business_id'],x['stars']))
    #joining the data
    joined_rdd = join_data(test_review_rdd,business_rdd)
    # averaging the data
    avg_rdd = average_rdd(joined_rdd)
    sorted_rdd = avg_rdd.takeOrdered(10, lambda x: (-x[1], x[0]))
    # sort
    top_10_items = [x[0] for x in sorted_rdd]
    print(top_10_items)

    spark_time_end = time.time()
    final_time_spark= spark_time_end-spark_time_start
    final_dictionary['m2']= final_time_spark
    final_dictionary['reason'] = "I compared pyspark and python and found that python's was faster. This is because python is better on small datasets due to its singgle-thread nature. However, on a large dataset pyspark would do better due to its ability ot leverage ditrubuted computing acrosss nodes."
    print(final_dictionary)


    # Write the results to a text file
    with open(output_filepath_1, 'w+') as f:
        f.write("city,stars\n")
        for record in sorted_average_rdd.collect():
            f.write(f"{record[0]},{record[1]}\n")
        f.close()
    # write dictionary to a json file
    with open(output_filepath_2, 'w+') as file:
        json.dump(final_dictionary, file)
        file.close()



