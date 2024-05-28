from datetime import datetime, timedelta
from typing  import List, Tuple
import pyspark
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import os 
import sys
from common_calculation import common_load_and_calculation

os.environ["HADOOP_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["YARN_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"



def datamart_by_users(base_path:str, date: str, depth: int, spark: SparkSession, geo_city_path: str) -> DataFrame:

    DAYS_COUNT = 27 # время непрерывного нахождения для определения домашнего города

    events, messages_with_city, user_act_location = common_load_and_calculation(base_path, date, depth, spark, geo_city_path)
     
    seq_num_window = Window().partitionBy("user_id").orderBy("message_dt")
    seq_num_city_window = Window().partitionBy("user_id", "city").orderBy("message_dt")
    user_city_visits = messages_with_city.withColumn("seq_num", F.row_number().over(seq_num_window)) \
                                         .withColumn("seq_num_city", F.row_number().over(seq_num_city_window)) \
                                         .withColumn("visit_num", F.col("seq_num") - F.col("seq_num_city"))

    user_city_visits = user_city_visits.groupBy("user_id", "city", "visit_num") \
                                       .agg(F.min("message_dt").alias("start_visit_dt"), \
                                            F.max("message_dt").alias("end_visit_dt"), \
                                           ) \
                                       .withColumn("visit_day_count", F.datediff(F.to_date("end_visit_dt"), F.to_date("start_visit_dt")))

    home_city_window = Window().partitionBy("user_id").orderBy(F.desc("start_visit_dt"))
    user_home_city = user_city_visits.where(f"visit_day_count >= {DAYS_COUNT}") \
                                     .withColumn("home_city_rank", F.row_number().over(home_city_window)) \
                                     .where("home_city_rank = 1") \
                                     .selectExpr("user_id",
                                                 "city AS home_city")             
    
    user_travel = user_city_visits.orderBy("start_visit_dt") \
                                  .groupBy("user_id") \
                                  .agg(F.count("city").alias("travel_count"), \
                                       F.collect_list("city").alias("travel_array")
                                      )                     
    
    user_datamart = user_act_location.join(user_travel, "user_id", "left") \
                                 .join(user_home_city, "user_id", "left") \
                                 .select("user_id",
                                         "act_city",
                                         "home_city",
                                         "travel_count",
                                         "travel_array",
                                         "local_time")
    # отладочный вывод убрал
    return user_datamart



def main():

    date = sys.argv[1]
    depth = int(sys.argv[2])
    in_base_path = sys.argv[3] # /user/yurgen001/data/geo/events
    out_base_path = sys.argv[4] # /user/yurgen001/data/analytics
    geo_city_path = sys.argv[5] # /user/yurgen001/data/snapshots/geo_city

    spark = SparkSession.builder \
                        .master("yarn") \
                        .appName(f"dmByUserJob-{date}-d{depth}") \
                        .config("spark.executor.memory", "2g") \
                        .config("spark.executor.cores", "2") \
                        .getOrCreate()

    datamart_by_users(in_base_path, date, depth, spark, geo_city_path).repartition(1).write \
                                                            .mode('overwrite') \
                                                            .parquet(f'{out_base_path}/dm_by_user_d{depth}/date={date}')

    
if __name__ == "__main__":
    main()