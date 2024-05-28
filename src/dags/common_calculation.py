from datetime import datetime, timedelta
from typing  import List, Tuple
import pyspark
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import os 
import sys



def input_paths(date: str, depth: int, base_path: str) -> List:
    dt = datetime.strptime(date, '%Y-%m-%d').date()
    path_list = [base_path + '/date=' + (dt - timedelta(days=i)).strftime('%Y-%m-%d')
                 for i in range(depth)
                ]
    return path_list


def common_load_and_calculation(base_path:str, date: str, depth: int, spark: SparkSession, geo_city_path: str) -> Tuple[DataFrame]:

    EARTH_RADIUS = 6371

    events_path = input_paths(date, depth, base_path)
    events = spark.read.option('basePath', base_path).parquet(*events_path)

    schema_geo_csv = StructType([ 
        StructField("id",IntegerType(),True), 
        StructField("city",StringType(),True), 
        StructField("lat",StringType(),True), 
        StructField("lng",StringType(),True)
    ])    
    
    geo_city = spark.read.options(delimiter=";", header=True) \
                         .schema(schema_geo_csv) \
                         .csv(geo_city_path) \
                         .withColumn("lat", F.regexp_replace("lat", ",", ".").cast(DoubleType())) \
                         .withColumn("lng", F.regexp_replace("lng", ",", ".").cast(DoubleType()))

       
    
    messages = events.where("event_type = 'message' AND event.message_ts IS NOT NULL") \
                     .selectExpr("event.message_from AS user_id",
                                 "event.message_ts AS message_dt",
                                 "lat AS message_lat",
                                 "lon AS message_lon") \
                     .crossJoin(geo_city.withColumnRenamed("lat", "city_lat") \
                                        .withColumnRenamed("lng", "city_lon")) 

  

    
    messages_with_distance = messages.withColumn("distance", 2 * EARTH_RADIUS * F.asin(F.sqrt(F.pow(F.sin((F.radians('message_lat') - F.radians('city_lat')) / F.lit(2)), F.lit(2)) \
                    + F.cos(F.radians('message_lat')) * F.cos(F.radians('city_lat')) * F.pow(F.sin((F.radians('message_lon') - F.radians('city_lon')) / F.lit(2)), F.lit(2))))
                                                )
    
    distance_rank_window = Window().partitionBy("user_id", "message_dt").orderBy("distance")
    messages_with_city = messages_with_distance.select("user_id",
                                                       "message_dt",
                                                       "message_lat",
                                                       "message_lon",
                                                       "city",
                                                       "distance") \
                                               .withColumn("distance_rank", F.row_number().over(distance_rank_window)) \
                                               .where("distance_rank = 1") \
                                               .select("user_id",
                                                       "message_dt",
                                                       "message_lat",
                                                       "message_lon",
                                                       "city")
    
    messages_with_city.cache()

     # переделывать временные зоны сил уже нет
     
    last_dt_window = Window().partitionBy("user_id").orderBy(F.desc("message_dt"))
    user_act_location = messages_with_city.withColumn("dt_rank", F.row_number().over(last_dt_window)) \
                                          .where("dt_rank = 1") \
                                          .selectExpr("user_id",
                                                      "message_lat AS act_lat",
                                                      "message_lon AS act_lon",
                                                      "city AS act_city", 
                                                      "CONCAT('Australia/', city) AS time_zone",
                                                      "FROM_UTC_TIMESTAMP(message_dt, CONCAT('Australia/', 'Sydney')) AS local_time"                            
                                                      )

    return (events,
            messages_with_city,
            user_act_location)

