from pyspark import SparkContext
import os
import time
import json
import sys

s=time.perf_counter()
# from pyspark.rdd import PipelinedRDD

# os.environ['PYTHON_SPARK'] = '/usr/local/bin/python3.7'
# os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.7'

# conf=SparkConf().setAppName("Task3_ExplorationonMultipleDatasets").setMaster("local[*]")
# sc=SparkContext.getOrCreate(conf)
sc = SparkContext('local[*]', 'Task3_ExplorationonMultipleDatasets')

reviewFile_path = sys.argv[1]
reviewFile = sc.textFile(reviewFile_path).map(json.loads)
businessFile_path = sys.argv[2]
businessFile = sc.textFile(businessFile_path).map(json.loads)


def swap(s):
    return (s[1], s[0])


def split_comma(s):
    return s.split(",")


def split_commandcotation(s):
    return s.split(',"')


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
    print(len(par))


def sumf(iterator):
    sum, count = 0, 0
    for v in iterator:
        sum = sum + v
        count = count + 1
    yield (sum, count)


reviewFile_RDD = reviewFile.map(lambda s: (s["business_id"], s["stars"])).persist()

businessFile_RDD = businessFile.map(lambda s: (s["business_id"], s["city"])).persist()


join_results = reviewFile_RDD.join(businessFile_RDD)
city_sum_count = join_results.map(lambda s: (s[1][1], s[1][0])).aggregateByKey((0, 0), lambda u, v: (u[0]+float(v), u[1]+1), lambda u1, u2: (u1[0]+u2[0], u1[1]+u2[1]))
average = city_sum_count.map(lambda s: (s[0], float(s[1][0])/s[1][1]))
# print(average.take(2))
# sort_average = average.map(swap).sortByKey(False).map(swap).sortByKey()
sort_average = average.sortByKey().sortBy(lambda s: s[1], False)
result = sort_average.collect()






# print(join_results)
# print(city_key)

start_m1 = time.perf_counter()
result_m1 = sort_average.collect()[:10]
# print(result_m1)
end_m1 = time.perf_counter()
m1 = end_m1-start_m1
# print(m1)



start_m2 = time.perf_counter()
result_m2=sort_average.take(10)
# print(result_m2)
end_m2 = time.perf_counter()
m2=end_m2-start_m2
# print(m2)


fileObject = open (sys.argv[3], 'w')
fileObject.write("city")
fileObject.write(",")
fileObject.write("stars")
fileObject.write("\n")
for i in result:
    fileObject.write(i[0])
    fileObject.write(",")
    fileObject.write(str(i[1]))
    fileObject.write("\n")
fileObject.close()

outCome = {"m1": m1, "m2": m2, "explanation": "When using RDD's collect() method, the data is loaded to the driver's memory which is time-consuming. However, take() retrieves only 10 in this case."}

with open(sys.argv[4], 'w') as f:
    json.dump(outCome, f)

e = time.perf_counter()
print(e-s)
