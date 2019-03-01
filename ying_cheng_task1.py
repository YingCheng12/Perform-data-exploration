from pyspark import SparkContext
import os
import json
import time
import sys

start_default = time.perf_counter()
# os.environ['PYTHON_SPARK'] = '/usr/local/bin/python3.7'
# os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.7'

sc = SparkContext('local[*]', 'Task1_DataExploration')

# sys.argv[1]

# reviewFile_path = '/Users/irischeng/PycharmProjects/Assignment1/yelp_dataset/review.json'
reviewFile_path = sys.argv[1]
reviewFile = sc.textFile(reviewFile_path)
# reviewFile.repartitionAndSortWithinPartitions()


def swap(s):
    return (s[1], s[0])


def split_comma(s):
    return s.split(",")


def add(a,b):
    return a+b


def split_colon(s):
    return s.split(":")


def strip_quotation(s):
    return s.strip("\"")


n_review = reviewFile.count()
# print(n_review)

n_review_2018 = reviewFile.filter(lambda s: '"date":"2018-' in s).count()
# print(n_review_2018)

userList_RDD = reviewFile.map(split_comma).map(lambda s: (s[1].split(":")[1].strip("\"")))
n_user = userList_RDD.distinct().count()
# print(n_user)
#
user_reviews_list = reviewFile.map(split_comma).map(
    lambda s: (s[1].split(":")[1].strip("\""), 1)).reduceByKey(add)
top10_user = user_reviews_list.sortByKey().map(swap).sortByKey(False).map(swap).take(10)
# print(top10_user)

busList_RDD = reviewFile.map(split_comma).map(lambda s: (s[2].split(":")[1].strip("\"")))
n_business = busList_RDD.distinct().count()
# print(n_business)

bus_reviews_list = reviewFile.map(split_comma).map(
    lambda s: (s[2].split(":")[1].strip("\""), 1)).reduceByKey(add)
top10_business = bus_reviews_list.sortByKey().map(swap).sortByKey(False).map(swap).take(10)
# print(top10_business)

outCome = {"n_review": n_review, "n_review_2018": n_review_2018, "n_user": n_user, "top10_user": top10_user,
           "n_business": n_business, "top10_business": top10_business}


with open(sys.argv[2], 'w') as f:
    json.dump(outCome, f)

end_default = time.perf_counter()
exe_time_default = end_default - start_default
print(exe_time_default)







