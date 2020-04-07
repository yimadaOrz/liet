from pyspark import SparkContext
import csv
import sys


if __name__=='__main__':
      
    sc = SparkContext()
    
    input_file = sys.argv[1]
    output_folder = sys.argv[2]
    
    lines = sc.textFile(input_file).cache() 
    header = lines.take(1)
    lines = lines.filter(lambda row: row not in header) \
            .mapPartitions(lambda line: csv.reader(line, delimiter=',', quotechar='"')) \
            .filter(lambda row: len(row) > 7 and type(row[0]) == str) \
            .filter(lambda row: len(row[0]) == 10) \
            .map(lambda line: ((line[1], line[0][:4], line[7].lower()),1)) \
            .reduceByKey(lambda x,y: x + y) \
            .map(lambda x: ((x[0][0], x[0][1]), (x[0][2], x[1]))) \
            .reduceByKey(lambda x,y: x + y) \
            .map(lambda x: (x[0][0], x[0][1], sum(i for i in x[1][1::2]), 
                                len(x[1][::2]),
                                round(100*max(i for i in x[1][1::2])/sum(i for i in x[1][1::2]))
                           )) \
            .sortBy(lambda x: (x[0][0], x[0][1])) \
            .saveAsTextFile(output_folder)