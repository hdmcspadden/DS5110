# import libraries
# need spark for a spark session
import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) != 2:
        # wrong number of args passed in
        print("usage: mnmcount <file>", file=sys.stderr)
        sys.exit(-1)

# Build/Start a SparkSession using Sparksession APIs
# only one spark session for JVM

spark = (SparkSession
    .builder
    .appName("PythonMnMCount")
    .getOrCreate())

# Get the M&M data set filename from the command-line arguments
mnm_file = sys.argv[1]

# Read the file into a Spark DataFrame using the csv
# format by inferring the schema and specifying that the file contains a head, which provids the column names
mnm_df = (spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(mnm_file))

# show 5 rows of dataframe with no truncation
mnm_df.show(n=5, truncate=False)

# aggregate count of all colors and groupby state and color - order descending sum of items
count_mnm_df = (mnm_df.select("State", "Color", "Count")
    .groupBy("State", "Color")
    .sum("Count")
    .orderBy("sum(Count)", ascending=False))

# now show the resulting aggregation for all the dates and colors
count_mnm_df.show(n=60, truncate=False)
print("Total Rows = %d" % (count_mnm_df.count()))

# find the aggrefate count for California
ca_count_mnm_df = (mnm_df.select("*")
    .where(mnm_df.State == 'CA')
    .groupBy("State", "Color")
    .sum("Count")
    .orderBy("sum(Count)", ascending=False))

# show the resulting aggregation for California
ca_count_mnm_df.show(n=10, truncate=False)