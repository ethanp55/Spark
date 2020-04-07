import sys
import re
from sys import stderr
from operator import add

from pyspark import SparkContext

def wordsFromLine(line):
    words = re.sub('[0-9]', '', line)
    words = words.lower()
    return re.split('[ ,:.;?!]', words)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: wordcount <input> <output>")
        exit(-1)
        
    sc = SparkContext.getOrCreate()
    
    lines = sc.textFile(sys.argv[1])
    
    counts = lines.flatMap(wordsFromLine).map(lambda word: (word, 1)).reduceByKey(add).sortBy(lambda pair: pair[1], ascending = False)
    
    print("Saving output")
    
    output = counts.collect()
    
#     for word, count in output:
#         print("%s: %i" % (word, count))
        
    counts.saveAsTextFile(sys.argv[2])
    
    sc.stop()
