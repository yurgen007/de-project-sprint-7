from datetime import datetime,  timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

default_args = {
    'owner': 'airflow',
    'start_date':datetime(2020, 1, 1),
}

# стартуем после полуночи и запускаем даг на перекладку таблицы  событий за прошедшие сутки
# после этого запускаем расчет витрин от вчерашней даты на заданную глубину
# тестовая глубина 60 дней

date = datetime.strptime({{ ds }}, '%Y-%m-%d').date() - timedelta(days=1) 
#date = '2022-05-31' 

with  DAG(
    dag_id = "friends_recomendation_dag",
    default_args=default_args,
    schedule_interval= '17 0 * * *',
) as dag_spark:

    events_geo_partitioning_job = SparkSubmitOperator(
        task_id=f'EventsGeoPartitioningJob-{date}',
        application ='partition_geo.py' ,
        conn_id= 'yarn_spark',
        application_args = [
            date,
            "/user/master/data/geo/events",
            "/user/yurgen001/data/geo/events"
        ],
        executor_cores = 2,
        executor_memory = '2g'
    )

    dm_by_user_job_d60 = SparkSubmitOperator(
        task_id=f'dmByUserJob-{date}-d60',
        application ='dm_by_user.py' ,
        conn_id= 'yarn_spark',
        application_args = [
            date,
            '60',
            '/user/yurgen001/data/geo/events',
            '/user/yurgen001/data/analytics',
            '/user/yurgen001/data/snapshots/geo_city'
        ],
        executor_cores = 2,
        executor_memory = '2g'
    )

    dm_by_zone_job_d60 = SparkSubmitOperator(
        task_id=f'dmByZoneJob-{date}-d60',
        application ='dm_by_zone.py' ,
        conn_id= 'yarn_spark',
        application_args = [
            date,
            '60',
            '/user/yurgen001/data/geo/events',
            '/user/yurgen001/data/analytics',
            '/user/yurgen001/data/snapshots/geo_city'
        ],
        executor_cores = 2,
        executor_memory = '2g'
    )


    dm_friends_recomendation_job_d60 = SparkSubmitOperator(
        task_id=f'dmFriendsRecomendationJob-{date}-d60',
        application ='dm_friends_recomendation.py' ,
        conn_id= 'yarn_spark',
        application_args = [
            date,
            '60',
            '/user/yurgen001/data/geo/events',
            '/user/yurgen001/data/analytics',
            '/user/yurgen001/data/snapshots/geo_city'
        ],
        executor_cores = 2,
        executor_memory = '2g'
    )

    events_geo_partitioning_job >> [dm_by_user_job_d60, dm_by_zone_job_d60, dm_friends_recomendation_job_d60]