import sys
 
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

import os 
os.environ["HADOOP_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["YARN_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"


def main():
    date = sys.argv[1]
    base_input_path = sys.argv[2]
    base_output_path = sys.argv[3]

    spark = SparkSession.builder \
                        .master("yarn") \
                        .appName(f"GeoEventsPartitioningJob-{date}") \
                        .config("spark.executor.memory", "2g") \
                        .config("spark.executor.cores", "2") \
                       .getOrCreate()


    events = spark.read.option('basePath', base_input_path).parquet(f"{base_input_path}/date={date}")


    events.write.option("header",True) \
            .partitionBy("date", "event_type") \
            .mode("overwrite") \
            .parquet(f'{base_output_path}/date={date}') 
    

if __name__ == "__main__":
    main()