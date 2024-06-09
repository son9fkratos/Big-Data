import sys
import re
from operator import add
from pyspark import SparkContext

if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.stderr.write("Usage: spark-submit <python file> <input_file> <output_file>\n")
        exit(-1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    sc = SparkContext(appName="PythonCleanWordCount")
    lines = sc.textFile(input_file, 1)
    
    # Remove special symbols and count words
    words = lines.flatMap(lambda x: re.findall(r'\b\w+\b', x))
    counts = words.map(lambda x: (x, 1)).reduceByKey(add)
    
    # Get the 10 most popular words
    top_10 = counts.takeOrdered(10, key=lambda x: -x[1])
    
    # Write the top 10 words to the output file
    with open(output_file, "w") as f:
        for (word, count) in top_10:
            f.write("%s: %i\n" % (word, count))
    
    sc.stop()
