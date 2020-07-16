from __future__ import print_function

import sys


from pyspark import SparkContext


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: wordcount <file> <output> ", file=sys.stderr)
        exit(-1)
        
    sc = SparkContext(appName="Top10Taxi")
    lines = sc.textFile(sys.argv[1], 1)
    taxilines = lines.map(lambda line: line.split(","))
    
    #clean up 
    #handling wrong data lines
    def isfloat(val):
        try: 
            float(val)
            return True
        except:
            return False
        
    #use isFloat to remove lines if they dont have 16 values and a float value !=0 for col 6 and col 12
    def correctRows(p):
        if(len(p)==17):
            if(isfloat(p[5]) and isfloat(p[11])):
                if(float(p[5])!=0 and float(p[11])!=0):
                    return p
                
    #go thru each element per row and keep only the correctRows
    cleanTaxiDT = taxilines.filter(correctRows)
    
    #set up each row with an 1 index
    rdd_1 = cleanTaxiDT.map(lambda x: ((x[0], x[1]), 1))
    
    #reduce, aggrgate by the combination of taxi and taxi drivers into a list
    rdd_1_reduce = rdd_1.reduceByKey(lambda x, y: x+y)
    
    # reduce in RDD to count taxi and most frequently used taxi
    by_medalionRDD2 = rdd_1_reduce.map(lambda x: (x[0][0],1)).reduceByKey(lambda x, y: x+y)
    
    #return the top 10 most used taxi by the count
    top10taxi = sc.parallelize(by_medalionRDD2.top(10, lambda x: x[1]))
    
    top10taxi.coalesce(1).saveAsTextFile(sys.argv[2])

    
    output = top10taxi.collect()
    
    sc.stop()
