from datetime import datetime, timedelta
from typing  import List, Tuple
import pyspark
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import os 
import sys

os.environ["HADOOP_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["YARN_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"



def input_paths(date: str, depth: int, base_path: str) -> List:
    dt = datetime.strptime(date, '%Y-%m-%d').date()
    path_list = [base_path + '/date=' + (dt - timedelta(days=i)).strftime('%Y-%m-%d')
                 for i in range(depth)
                ]
    return path_list


def common_load_and_calculation(base_path:str, date: str, depth: int, spark: SparkSession, geo_city_path: str) -> Tuple[DataFrame]:

    EARTH_RADIUS = 6371

    events_path = input_paths(date, depth, base_path)
#     print(events_path)
    events = spark.read.option('basePath', base_path).parquet(*events_path)
#     events.printSchema()

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
#     geo_city.printSchema()
#     geo_city.show(10)

       
    
    messages = events.where("event_type = 'message' AND event.message_ts IS NOT NULL") \
                     .selectExpr("event.message_from AS user_id",
                                 "event.message_ts AS message_dt",
                                 "lat AS message_lat",
                                 "lon AS message_lon") \
                     .crossJoin(geo_city.withColumnRenamed("lat", "city_lat") \
                                        .withColumnRenamed("lng", "city_lon")) 

#     messages.show()
  

    
    messages_with_distance = messages.withColumn("distance", 2 * EARTH_RADIUS * F.asin(F.sqrt(F.pow(F.sin((F.radians('message_lat') - F.radians('city_lat')) / F.lit(2)), F.lit(2)) \
                    + F.cos(F.radians('message_lat')) * F.cos(F.radians('city_lat')) * F.pow(F.sin((F.radians('message_lon') - F.radians('city_lon')) / F.lit(2)), F.lit(2))))
                                                )
    
#     messages_with_distance.show()
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
#     messages_with_city.show()

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

    # user_act_location.show(20, False)
    return (events,
            messages_with_city,
            user_act_location)



def datamart_by_users(messages_with_city: DataFrame,
                      user_act_location: DataFrame) -> DataFrame:

    DAYS_COUNT = 27 # время непрерывного нахождения для определения домашнего города
     
    seq_num_window = Window().partitionBy("user_id").orderBy("message_dt")
    seq_num_city_window = Window().partitionBy("user_id", "city").orderBy("message_dt")
    user_city_visits = messages_with_city.withColumn("seq_num", F.row_number().over(seq_num_window)) \
                                         .withColumn("seq_num_city", F.row_number().over(seq_num_city_window)) \
                                         .withColumn("visit_num", F.col("seq_num") - F.col("seq_num_city"))

#     user_city_visits.show(40, False)
    user_city_visits = user_city_visits.groupBy("user_id", "city", "visit_num") \
                                       .agg(F.min("message_dt").alias("start_visit_dt"), \
                                            F.max("message_dt").alias("end_visit_dt"), \
                                           ) \
                                       .withColumn("visit_day_count", F.datediff(F.to_date("end_visit_dt"), F.to_date("start_visit_dt")))
#     user_city_visits.show(40, False)

    home_city_window = Window().partitionBy("user_id").orderBy(F.desc("start_visit_dt"))
    user_home_city = user_city_visits.where(f"visit_day_count >= {DAYS_COUNT}") \
                                     .withColumn("home_city_rank", F.row_number().over(home_city_window)) \
                                     .where("home_city_rank = 1") \
                                     .selectExpr("user_id",
                                                 "city AS home_city")             
#     user_home_city.show()
    
    user_travel = user_city_visits.orderBy("start_visit_dt") \
                                  .groupBy("user_id") \
                                  .agg(F.count("city").alias("travel_count"), \
                                       F.collect_list("city").alias("travel_array")
                                      )                     
#     user_travel.show(40, False)
    
    user_datamart = user_act_location.join(user_travel, "user_id", "left") \
                                 .join(user_home_city, "user_id", "left") \
                                 .select("user_id",
                                         "act_city",
                                         "home_city",
                                         "travel_count",
                                         "travel_array",
                                         "local_time")
    user_datamart.show()

    return user_datamart



def datamart_by_zone(events: DataFrame,
                     messages_with_city: DataFrame,
                     user_act_location: DataFrame) -> DataFrame:


    out_messages = messages_with_city.selectExpr("TO_DATE(message_dt) AS event_date",
                                                 "'message' AS event_type",
                                                 "city AS zone_id")
    
#     out_messages.show()


    out_reactions = events.where("event_type = 'reaction'") \
                      .selectExpr("CAST(event.reaction_from AS LONG) AS user_id",
                                  "TO_DATE(event.datetime) AS event_date") \
                      .join(user_act_location, "user_id", "inner") \
                      .selectExpr("event_date",
                                  "'reaction' AS event_type",
                                  "act_city AS zone_id")
    
#     out_reactions.printSchema()
#     out_reactions.show()
    

    out_subscriptions = events.where("event_type = 'subscription'") \
                              .selectExpr("CAST(event.user AS LONG) AS user_id",
                                          "TO_DATE(event.datetime) AS event_date") \
                              .join(user_act_location, "user_id", "inner") \
                              .selectExpr("event_date",
                                          "'subscription' AS event_type",
                                          "act_city AS zone_id")
    
#     out_subscriptions.printSchema()
#     out_subscriptions.show()
    
    out_registrations = events.where("event_type = 'registration'") \
                              .selectExpr("CAST(event.user AS LONG) AS user_id",
                                          "TO_DATE(event.datetime) AS event_date") \
                              .join(user_act_location, "user_id", "inner") \
                              .selectExpr("event_date",
                                          "'registration' AS event_type",
                                          "act_city AS zone_id")

#     out_registrations.printSchema()
#     out_registrations.show()

    all_events =  out_messages.unionByName(out_reactions) \
                              .unionByName(out_subscriptions) \
                              .unionByName(out_registrations) \
                              .withColumn("month", F.month("event_date")) \
                              .withColumn("week", F.expr("FLOOR(DAYOFMONTH(event_date) / 7) + 1"))
    
#     all_events.show()

    zone_datamart_by_week = all_events.groupBy("zone_id", "month", "week") \
                                      .pivot("event_type", ["message", "reaction", "subscription", "registration"]) \
                                      .agg(F.count("event_date")) \
                                      .withColumnRenamed("message", "week_message") \
                                      .withColumnRenamed("reaction", "week_reaction") \
                                      .withColumnRenamed("subscription", "week_subscription") \
                                      .withColumnRenamed("registration", "week_user")
                                      
    
#     zone_datamart_by_week.show(100)
    
    zone_datamart_by_month = all_events.groupBy("zone_id", "month") \
                                       .pivot("event_type", ["message", "reaction", "subscription", "registration"]) \
                                       .agg(F.count("event_date")) \
                                       .withColumnRenamed("message", "month_message") \
                                       .withColumnRenamed("reaction", "month_reaction") \
                                       .withColumnRenamed("subscription", "month_subscription") \
                                       .withColumnRenamed("registration", "month_user")
                                      
    
#     zone_datamart_by_month.show(100)
    
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
                                         .na.fill(value=0)
    zone_datamart.show()
    return zone_datamart


def datamart_friends_recomendation(events:DataFrame,
                                   user_act_location: DataFrame) -> DataFrame:

    EARTH_RADIUS = 6371

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

#     neighbors.show()

    messages_from_left_user = events.where("event_type = 'message'") \
                                    .selectExpr("event.message_from AS message_from",
                                                "event.message_to AS message_to") \
                                    .join(neighbors.select("left_user"), neighbors["left_user"] == F.col("message_from"), "inner") \
                                    .selectExpr("left_user",
                                                "message_to AS right_user")

#     messages_from_left_user.show()

    messages_to_left_user = events.where("event_type = 'message'") \
                                 .selectExpr("event.message_from AS message_from",
                                             "event.message_to AS message_to") \
                                 .join(neighbors.select("left_user"), neighbors["left_user"] == F.col("message_to"), "inner") \
                                 .selectExpr("left_user",
                                             "message_from AS right_user")
#     messages_to_left_user.show()

    left_user_correspondents = messages_from_left_user.unionByName(messages_to_left_user) \
                                                      .na.drop()
    
#     left_user_correspondents.show()

    neighbors_without_messages = neighbors.join(left_user_correspondents, ["left_user", "right_user"], "leftanti") 
    
    neighbors_without_messages.cache()
#     neighbors_without_messages.show()
    
    left_user_subscriptions = events.where("event_type = 'subscription'") \
                             .selectExpr("CAST(event.user AS LONG) AS left_user",
                                         "event.subscription_channel AS subscription_channel") \
                             .join(neighbors_without_messages.select("left_user"), ["left_user"], "inner") \
                             .selectExpr("left_user",
                                         "subscription_channel")

#     left_user_subscriptions.show()
    
    right_user_subscritions = events.where("event_type = 'subscription'") \
                              .selectExpr("CAST(event.user AS LONG) AS right_user",
                                          "event.subscription_channel AS subscription_channel") \
                              .join(neighbors_without_messages.select("right_user"), ["right_user"], "inner") \
                              .selectExpr("right_user",
                                          "subscription_channel")
    
#     right_user_subscritions.show()
    
    common_subscribers = left_user_subscriptions.join(right_user_subscritions, ["subscription_channel"], "inner") \
                                         .select("left_user",
                                                 "right_user")
    
#     common_subscribers.show()

    friends_recomendation = neighbors_without_messages.join(common_subscribers, ["left_user", "right_user"], "leftsemi")

    friends_recomendation.show()
    return friends_recomendation
    

def datamarts_calculation(base_path:str, date: str, depth: int, spark: SparkSession, out_base_path: str, geo_city_path: str) -> None:
    
    events, messages_with_city, user_act_location = common_load_and_calculation(base_path, date, depth, spark, geo_city_path)

    datamart_by_users(messages_with_city, user_act_location).write \
                                                            .mode('overwrite') \
                                                            .parquet(f'{out_base_path}/dm_by_user_d{depth}/date={date}')

    datamart_by_zone(events, messages_with_city, user_act_location).write \
                                                                   .mode('overwrite') \
                                                                   .parquet(f'{out_base_path}/dm_by_zone_d{depth}/date={date}')

    datamart_friends_recomendation(events, user_act_location).write \
                                                             .mode('overwrite') \
                                                             .parquet(f'{out_base_path}/dm_friends_recomendation_d{depth}/date={date}')



def main():

    date = sys.argv[1]
    depth = int(sys.argv[2])
    in_base_path = sys.argv[3] # /user/yurgen001/data/geo/events
    out_base_path = sys.argv[4] # /user/yurgen001/data/analytics
    geo_city_path = sys.argv[5] # /user/yurgen001/data/snapshots/geo_city

    spark = SparkSession.builder \
                        .master("yarn") \
                        .appName(f"FriendsRecomendationJob-{date}-d{depth}") \
                        .config("spark.executor.memory", "2g") \
                        .config("spark.executor.cores", "2") \
                        .getOrCreate()

    datamarts_calculation(in_base_path, date, depth, spark, out_base_path, geo_city_path)
    
if __name__ == "__main__":
    main()