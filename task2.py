from pyspark import SparkContext
import sys
import time
import json

def read_data(file_path):
    sc= SparkContext('local','task2')    
    rdd = sc.textFile(file_path).map(lambda x: json.loads(x))
    return rdd

def top_10(rdd):
    ordered_rdd = rdd.reduceByKey(lambda a,b: a+b).sortBy(lambda x: x[1]).take(10)
    rdd_list = [list(user) for user in ordered_rdd]
    return rdd_list

if __name__ == '__main__':
    # getting the paths 
    review_filepath = sys.argv[1]
    output_filepath = sys.argv[2]
    num_partition = int(sys.argv[3])


    # reading in the data 
    rdd = read_data(review_filepath)
    default = rdd.map(lambda record: (record['business_id'],1))
    custom = rdd.map(lambda record: (record['business_id'],1)).partitionBy(num_partition,lambda key: hash(key) % num_partition)

    final_dictionary= {}
    default_intermediate_dictionary = {}
    custom_intermediate_dictionary = {}


    # default 
    default_num_partitions = default.getNumPartitions()
    default_length_partitions = default.glom().map(len).collect()

    # seeing the end time - start time 
    def_begin_time = time.time()
    default_rdd = top_10(default)
    def_end_time = time.time()
    result_default = def_end_time - def_begin_time

    # storing the intermediate results
    default_intermediate_dictionary['n_partition'] = default_num_partitions
    default_intermediate_dictionary['n_items'] = default_length_partitions
    default_intermediate_dictionary['exe_time'] = result_default

    # storing all the results in the final dictionary
    final_dictionary['default']= default_intermediate_dictionary


     # custom 
    custom_num_partitions = custom.getNumPartitions()
    custom_length_partitions = custom.glom().map(len).collect()

    # seeing the end time - start time 
    custom_begin_time = time.time()
    custom_rdd = top_10(custom)
    custom_end_time = time.time()
    result_custom= custom_end_time - custom_begin_time

    # storing the intermediate results
    custom_intermediate_dictionary['n_partition'] = custom_num_partitions
    custom_intermediate_dictionary['n_items'] = custom_length_partitions
    custom_intermediate_dictionary['exe_time'] = result_custom

    # storing all the results in the final dictionary
    final_dictionary['customized']= custom_intermediate_dictionary

    with open(output_filepath, 'w+') as file:
        json.dump(final_dictionary, file)
        file.close()








    