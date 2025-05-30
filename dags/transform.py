# Write content of a dag file that 1st task is reading the players.csv file in the rootfolder/include/dataset/202505 directory using pyspark to a dataframe and then group by "hero_id" and "patch" then calculate all the average of gold_per_min,xp_per_min,kills_per_min,last_hits_per_min,hero_damage_per_min,hero_healing_per_min,tower_damage

from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

@dag(schedule_interval=None, catchup=False)
def extract_and_load():

    read_and_store_heroes = SparkSubmitOperator(
        task_id='aggregate_hero_stats',
        application='./include/scripts/aggregate_hero_stats.py',
        name='Aggregate Hero Stats',
        conn_id='my_spark_conn',
        verbose=True,
    )

    read_and_store_heroes

extract_and_load()