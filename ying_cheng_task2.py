from pyspark import SparkContext
import os
import time
import json
import sys

from pyspark.rdd import PipelinedRDD


sc = SparkContext('local[*]', 'Task2_Partition')

reviewFile_path = sys.argv[1]
reviewFile_default = sc.textFile(reviewFile_path)
reviewFile_customized = sc.textFile(reviewFile_path, int(sys.argv[3]))



def swap(s):
    return (s[1], s[0])


def split_comma(s):
    return s.split(",")


def add(a, b):
    return a + b


def split_colon(s):
    return s.split(":")


def strip_quotation(s):
    return s.strip("\"")


def printf(s):
    par = list(s)
    print(par)


def count(s):
    par = list(s)
    yield (len(par))


def sumf(iterator):
    sum, count = 0, 0
    for v in iterator:
        sum = sum + v
        count = count + 1
    yield (sum, count)



start_default = time.perf_counter()
n_partition_default = reviewFile_default.getNumPartitions()
# print(n_partition_default)
n_items_default = reviewFile_default.map(lambda s: s[0]).mapPartitions(count).collect()
bus_reviews_list_default: PipelinedRDD = reviewFile_default.map(split_comma).map(
    lambda s: (s[2].split(":")[1].strip("\""), 1)).reduceByKey(add)

# n_items_default = (bus_reviews_list_default.map(lambda s: s[0]).foreachPartition(count))
# print(n_items_default)
# n_items_default = bus_reviews_list_default.map(lambda s: s[0]).mapPartitions(count).collect()
# print(n_items_default)
top10_business_default = bus_reviews_list_default.sortByKey().map(swap).sortByKey(False).map(swap).take(10)
# print(top10_business_default)
end_default = time.perf_counter()
exe_time_default = end_default - start_default
# print(exe_time_default)



start_customized = time.perf_counter()
n_partition_customized = reviewFile_customized.getNumPartitions()
n_items_customized = reviewFile_customized.map(lambda s: s[0]).mapPartitions(count).collect()
# print(n_partition_customized)
bus_reviews_list_customized = reviewFile_customized.map(split_comma).map(
    lambda s: (s[2].split(":")[1].strip("\""), 1)).reduceByKey(add)
# n_items_customized = bus_reviews_list_customized.map(lambda s: s[0]).mapPartitions(count).collect()
# print(n_items_customized)

top10_business_customized = bus_reviews_list_customized.sortByKey().map(swap).sortByKey(False).map(swap).take(10)
# print(top10_business_customized)
end_customized = time.perf_counter()

exe_time_customized = end_customized-start_customized
# print(exe_time_customized)

outCome = {"default": {"n_partition": n_partition_default, "n_items": n_items_default, "exe_time": exe_time_default},
           "customized": {"n_partition": n_partition_customized, "n_items": n_items_customized, "exe_time": exe_time_customized},
           "explanation": "The one whose execution time is shorter reduces the shuffling time between transformation and action."}


with open(sys.argv[2], 'w') as f:
    json.dump(outCome, f)

