from __future__ import print_function

import sys

from pyspark import SparkContext
from datetime import datetime

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: wordcount <file> <output> ", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="bestTime")
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
    
    # create a method to extract hour
    def getHour(x):
      dtObj = datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
      return dtObj.strftime('%H')
    
    #create aRDD with the relevant columns: pick up date, surcharge, distance
    pt3RDD = cleanTaxiDT.map(lambda x: (getHour(x[2]), (float(x[12]), float(x[5]))))
    #RDD to contain sum of all surcharge, all distance for each date as key
    byHourRDD = pt3RDD.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
    #RDD to produce date and profit ratio by day
    bestDayRDD = byHourRDD.map(lambda x: (x[0], x[1][0]/x[1][1]))

    bestWrkHr = sc.parallelize(bestDayRDD.top(10, lambda x: x[1]))

    bestWrkHr.coalesce(1).saveAsTextFile(sys.argv[2])
    
    
    sc.stop()
