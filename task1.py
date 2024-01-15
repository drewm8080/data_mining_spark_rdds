from pyspark import SparkContext
import sys
import json


def read_data(file_path):
    sc= SparkContext('local[*]','task1')    
    rdd = sc.textFile(file_path).map(lambda x: json.loads(x))
    return rdd

def filter_reviews(rdd):
    filtered_rdd_count = rdd.filter(lambda record: record["date"][0:4] == "2018").count()
    return filtered_rdd_count

def distinct(rdd,key):
    distinct_rdd = rdd.map(lambda record: record[key]).distinct().count()
    return distinct_rdd

def top_10(rdd,key):
    ordered_rdd = rdd.map(lambda record: (record[key],1)).reduceByKey(lambda a,b: a+b).sortBy(lambda x: x[1]).take(10)
    rdd_list = [list(user) for user in ordered_rdd]
    return rdd_list

    


if __name__ == '__main__':
    # getting the paths 
    review_filepath = sys.argv[1]
    output_filepath = sys.argv[2]
    
    # reading in the data 
    rdd = read_data(review_filepath)
    
    # computing the answers
    count = rdd.count()
    total_reviews_2018 = filter_reviews(rdd)
    distinct_users_count = distinct(rdd,'user_id')
    topten_cus = top_10(rdd,'user_id')
    distinct_business_count = distinct(rdd,'business_id')
    topten_bus = top_10(rdd,'business_id')
    # getting final results in the right format
    final_dict = {}
    final_dict["n_review"] = count
    final_dict["n_review_2018"] = total_reviews_2018
    final_dict["n_user"] = distinct_users_count
    final_dict["top10_user"] = topten_cus
    final_dict["n_business"] = distinct_business_count
    final_dict["top10_business"] = topten_bus


    # writing results
    with open(output_filepath, 'w+') as file:
        json.dump(final_dict, file)
        file.close()





