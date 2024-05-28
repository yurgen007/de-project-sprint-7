from datetime import datetime, timedelta
from typing  import List, Tuple
import pyspark
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
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



def datamart_by_zone(base_path:str, date: str, depth: int, spark: SparkSession, geo_city_path: str) -> DataFrame:

    events, messages_with_city, user_act_location = common_load_and_calculation(base_path, date, depth, spark, geo_city_path)


    out_messages = messages_with_city.selectExpr("TO_DATE(message_dt) AS event_date",
                                                 "'message' AS event_type",
                                                 "city AS zone_id")
    


    out_reactions = events.where("event_type = 'reaction'") \
                      .selectExpr("CAST(event.reaction_from AS LONG) AS user_id",
                                  "TO_DATE(event.datetime) AS event_date") \
                      .join(user_act_location, "user_id", "inner") \
                      .selectExpr("event_date",
                                  "'reaction' AS event_type",
                                  "act_city AS zone_id")
    
    

    out_subscriptions = events.where("event_type = 'subscription'") \
                              .selectExpr("CAST(event.user AS LONG) AS user_id",
                                          "TO_DATE(event.datetime) AS event_date") \
                              .join(user_act_location, "user_id", "inner") \
                              .selectExpr("event_date",
                                          "'subscription' AS event_type",
                                          "act_city AS zone_id")
    
    # Переделал расчет регистраций через первое сообщение пользователя

    first_dt_window = Window().partitionBy("user_id").orderBy("message_dt")
    user_first_message = messages_with_city.withColumn("dt_rank", F.row_number().over(first_dt_window)) \
                                          .where("dt_rank = 1") \
                                          .selectExpr("user_id",
                                                      "message_dt",
                                                      "city"
                                                      ) 
    
    out_registrations = user_first_message.selectExpr("TO_DATE(message_dt) AS event_date",
                                                      "'registration' AS event_type",
                                                      "city AS zone_id")

    # Переделал расчет месяца и недели через функцию date_trunc

    all_events =  out_messages.unionByName(out_reactions) \
                              .unionByName(out_subscriptions) \
                              .unionByName(out_registrations) \
                              .withColumn("week", F.date_trunc("week", "event_date")) \
                              .withColumn("month", F.date_trunc("month", "event_date"))

    zone_datamart_by_week = all_events.groupBy("zone_id", "month", "week") \
                                      .pivot("event_type", ["message", "reaction", "subscription", "registration"]) \
                                      .agg(F.count("event_date")) \
                                      .withColumnRenamed("message", "week_message") \
                                      .withColumnRenamed("reaction", "week_reaction") \
                                      .withColumnRenamed("subscription", "week_subscription") \
                                      .withColumnRenamed("registration", "week_user")
                                      
    
    zone_datamart_by_month = all_events.groupBy("zone_id", "month") \
                                       .pivot("event_type", ["message", "reaction", "subscription", "registration"]) \
                                       .agg(F.count("event_date")) \
                                       .withColumnRenamed("message", "month_message") \
                                       .withColumnRenamed("reaction", "month_reaction") \
                                       .withColumnRenamed("subscription", "month_subscription") \
                                       .withColumnRenamed("registration", "month_user")
                                      
    
   
    zone_datamart = zone_datamart_by_week.join(zone_datamart_by_month, ["zone_id", "month"], "inner") \
                                         .select("month",
                                                 "week",
                                                 "zone_id",
                                                 "week_message",
                                                 "week_reaction",
                                                 "week_subscription",
                                                 "week_user",
                                                 "month_message",
                                                 "month_reaction",
                                                 "month_subscription",
                                                 "month_user",
                                                ) \
                                         .orderBy("zone_id", "month", "week") \
                                         .na.fill(value=0)
    return zone_datamart




def main():

    date = sys.argv[1]
    depth = int(sys.argv[2])
    in_base_path = sys.argv[3] # /user/yurgen001/data/geo/events
    out_base_path = sys.argv[4] # /user/yurgen001/data/analytics
    geo_city_path = sys.argv[5] # /user/yurgen001/data/snapshots/geo_city

    spark = SparkSession.builder \
                        .master("yarn") \
                        .appName(f"dmByZoneJob-{date}-d{depth}") \
                        .config("spark.executor.memory", "2g") \
                        .config("spark.executor.cores", "2") \
                        .getOrCreate()

    datamart_by_zone(in_base_path, date, depth, spark, geo_city_path).repartition(1).write \
                                                                   .mode('overwrite') \
                                                                   .parquet(f'{out_base_path}/dm_by_zone_d{depth}/date={date}')
    
if __name__ == "__main__":
    main()