import sys
import re
from operator import add
from pyspark import SparkContext

if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.stderr.write("Usage: spark-submit <python script> <file>\n")
        exit(-1)
    sc = SparkContext(appName="PythonCleanWordCount")
    lines = sc.textFile(sys.argv[1], 1)
    words = lines.flatMap(lambda x: re.findall(r'\b\w+\b', x))
    counts = words.map(lambda x: (x, 1)).reduceByKey(add)
    output = counts.sortBy(lambda x: x[0]).collect()
    
    with open("clean_word_count.txt", "w") as f:
        for (word, count) in output:
            f.write("%s: %i\n" % (word, count))
    
    sc.stop()
