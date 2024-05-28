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




def datamart_friends_recomendation(base_path:str, date: str, depth: int, spark: SparkSession, geo_city_path: str) -> DataFrame:

    EARTH_RADIUS = 6371

    events, messages_with_city, user_act_location = common_load_and_calculation(base_path, date, depth, spark, geo_city_path)

    neighbors = user_act_location.alias("df_left").join(user_act_location.alias("df_right"), F.col("df_left.user_id") < F.col("df_right.user_id"), 'inner') \
        .withColumn("distance", 2 * EARTH_RADIUS * F.expr("asin(sqrt(pow(sin((radians(df_left.act_lat) - radians(df_right.act_lat)) / 2), 2)"
        " + cos(radians(df_left.act_lat)) * cos(radians(df_right.act_lat)) * pow(sin((radians(df_left.act_lon) - radians(df_right.act_lon)) / 2), 2)))")
                   ) \
                                .where("distance < 1") \
                                .selectExpr("df_left.user_id AS left_user",
                                            "df_right.user_id AS right_user",
                                            "CURRENT_TIMESTAMP() AS processed_dttm",
                                            "df_left.act_city AS zone_id",
                                            "GREATEST(df_left.local_time, df_right.local_time) AS local_time"
                                           )
# Старый вариант расчета по переписке пользователей

#     messages_from_left_user = events.where("event_type = 'message'") \
#                                     .selectExpr("event.message_from AS message_from",
#                                                 "event.message_to AS message_to") \
#                                     .join(neighbors.select("left_user"), neighbors["left_user"] == F.col("message_from"), "inner") \
#                                     .selectExpr("left_user",
#                                                 "message_to AS right_user")


#     messages_to_left_user = events.where("event_type = 'message'") \
#                                   .selectExpr("event.message_from AS message_from",
#                                              "event.message_to AS message_to") \
#                                   .join(neighbors.select("left_user"), neighbors["left_user"] == F.col("message_to"), "inner") \
#                                   .selectExpr("left_user",
#                                              "message_from AS right_user")

#     left_user_correspondents = messages_from_left_user.unionByName(messages_to_left_user) \
#                                                       .na.drop()
    

# Переделал расчет по переписывающимся пользователям. Поскольку neighbors содержит только уникальные пары user_id   
#  и left_user < right_user (условие джойна F.col("df_left.user_id") < F.col("df_right.user_id")),
# то корреспондент с меньшим id определяется как левый юзер, а с большим как правый 


    correspondents = events.where("event_type = 'message' AND event.message_from IS NOT NULL AND event.message_to IS NOT NULL") \
                           .selectExpr("""
                                          CASE 
                                            WHEN event.message_from < event.message_to THEN event.message_from
                                            ELSE event.message_to  
                                          END AS left_user
                                       """,
                                       """
                                         CASE 
                                            WHEN event.message_from > event.message_to THEN event.message_from
                                            ELSE event.message_to  
                                          END AS right_user
                                       """
                                     ) 

# Но джойн в итоге один

    neighbors_without_messages = neighbors.join(correspondents, ["left_user", "right_user"], "leftanti") 
    
    neighbors_without_messages.cache()

# Старая версия расчета витрины
    
#     left_user_subscriptions = events.where("event_type = 'subscription'") \
#                              .selectExpr("CAST(event.user AS LONG) AS left_user",
#                                          "event.subscription_channel AS subscription_channel") \
#                              .join(neighbors_without_messages.select("left_user"), ["left_user"], "inner") \
#                              .selectExpr("left_user",
#                                          "subscription_channel")

    
#     right_user_subscritions = events.where("event_type = 'subscription'") \
#                               .selectExpr("CAST(event.user AS LONG) AS right_user",
#                                           "event.subscription_channel AS subscription_channel") \
#                               .join(neighbors_without_messages.select("right_user"), ["right_user"], "inner") \
#                               .selectExpr("right_user",
#                                           "subscription_channel")
    
    
#     common_subscribers = left_user_subscriptions.join(right_user_subscritions, ["subscription_channel"], "inner") \
#                                          .select("left_user",
#                                                  "right_user")
    

    # friends_recomendation = neighbors_without_messages.join(common_subscribers, ["left_user", "right_user"], "leftsemi") \
    #                                                   .select("left_user",
    #                                                           "right_user",
    #                                                           "processed_dttm",
    #                                                           "zone_id",
    #                                                           "local_time") 

# Новая версия расчета витрины через объединение подписок в множество 

    user_subscriptions = events.where("event_type = 'subscription'") \
                               .selectExpr("CAST(event.user AS LONG) AS user_id",
                                           "event.subscription_channel AS subscription_channel") \
                               .groupBy("user_id") \
                               .agg(F.collect_set("subscription_channel").alias("subscription_set"))

# Написал явный селект

    friends_recomendation = neighbors_without_messages.join(user_subscriptions.alias("l_sub"), F.expr("left_user = l_sub.user_id"), "inner") \
                                                      .join(user_subscriptions.alias("r_sub"), F.expr("right_user = r_sub.user_id"), "inner") \
                                                      .where("SIZE(ARRAY_INTERSECT(l_sub.subscription_set, r_sub.subscription_set)) > 0") \
                                                      .select("left_user",
                                                              "right_user",
                                                              "processed_dttm",
                                                              "zone_id",
                                                              "local_time") 

    return friends_recomendation
    


def main():

    date = sys.argv[1]
    depth = int(sys.argv[2])
    in_base_path = sys.argv[3] # /user/yurgen001/data/geo/events
    out_base_path = sys.argv[4] # /user/yurgen001/data/analytics
    geo_city_path = sys.argv[5] # /user/yurgen001/data/snapshots/geo_city

    spark = SparkSession.builder \
                        .master("yarn") \
                        .appName(f"dmFriendsRecomendationJob-{date}-d{depth}") \
                        .config("spark.executor.memory", "2g") \
                        .config("spark.executor.cores", "2") \
                        .getOrCreate()

    datamart_friends_recomendation(in_base_path, date, depth, spark, geo_city_path).repartition(1).write \
                                                             .mode('overwrite') \
                                                             .parquet(f'{out_base_path}/dm_friends_recomendation_d{depth}/date={date}')
    
if __name__ == "__main__":
    main()