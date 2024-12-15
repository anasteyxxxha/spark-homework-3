import sys
from operator import add

from pyspark.sql import SparkSession


spark = SparkSession\
    .builder\
    .appName("PythonWordCount")\
    .getOrCreate()

lorem_ipsum = "lorem ipsum set amet"

lines = spark.read.text(lorem_ipsum).rdd.map(lambda r: r[0])
counts = lines.flatMap(lambda x: x.split(' ')) \
                .map(lambda x: (x, 1)) \
                .reduceByKey(add)
output = counts.collect()
for (word, count) in output:
    print("%s: %i" % (word, count))

spark.stop()