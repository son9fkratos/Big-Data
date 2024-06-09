import sys
from pyspark import SparkContext

if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.stderr.write("Usage: spark-submit <python script> <file>\n")
        exit(-1)
    sc = SparkContext(appName="PythonLineCount")
    lines = sc.textFile(sys.argv[1], 1)
    line_count = lines.count()
    
    with open("line_count.txt", "w") as f:
        f.write("Number of lines: %i\n" % line_count)
    
    sc.stop()
