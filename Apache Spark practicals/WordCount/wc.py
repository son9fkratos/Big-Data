import sys
from operator import add
from pyspark import SparkContext

if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.stderr.write("Usage: spark-submit <python script> <file>\n")
        exit(-1)
    sc = SparkContext(appName="PythonWordCount")
    lines = sc.textFile(sys.argv[1], 1)
    words = lines.flatMap(lambda x: x.split(' '))
    word_count = words.map(lambda x: 1).reduce(add)
    
    with open("word_count.txt", "w") as f:
        f.write("Number of words: %i\n" % word_count)
    
    sc.stop()