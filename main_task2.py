from __future__ import print_function

import sys

from pyspark import SparkContext


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: wordcount <file> <output> ", file=sys.stderr)
        exit(-1)
        
    sc = SparkContext(appName="10drivers")
    lines = sc.textFile(sys.argv[1])
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
    
    def correctRows2(p):
        if(float(p[4]) !=0):
            return p
    
    cleanTaxiDT2 = cleanTaxiDT.filter(correctRows2)
    
    #get a driver rdd and map a function to get the money per minute value
    by_drvr = cleanTaxiDT2.map(lambda x: (x[1], (float(x[11])/(float(x[4])/60), 1.0)))
    #aggregate by key=driverID and tke an average using the aggregate byByKey
    aggfun = lambda x, y: (x[0]+y[0], x[1]+y[1])
    rateRDD = by_drvr.aggregateByKey((0.0,0.0), aggfun, aggfun)
    
    avg_rateRDD = rateRDD.map(lambda x: (x[0], x[1][0]/x[1][1]))
    
    # Get top 10 drivers
    top10drivers = sc.parallelize(avg_rateRDD.top(10, lambda x: x[1]))
    top10drivers.coalesce(1).saveAsTextFile(sys.argv[2])
    
    output = top10drivers.collect()
    
    sc.stop()
