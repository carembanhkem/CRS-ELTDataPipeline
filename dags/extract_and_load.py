# # Write content of a dag file that 1st task is reading all the players.csv file in each subfolder inside the rootfolder/include/dataset directory using pyspark and save it to a parquet file in rootfolder/include/dataset/output/players.parquet, 2nd task is reading the players.parquet file and importing it to a HDFS location using the hdfs package in python.
# from airflow.decorators import dag, task
# from airflow.utils.dates import days_ago
# from pyspark.sql import SparkSession
# import os
# from pyspark.sql import DataFrame
# from pyspark.sql.functions import col
# @dag(schedule_interval=None, start_date=days_ago(1), catchup=False, tags=['elt_pipeline'])
# def extract_and_load():
# # Task 1: Read the players.csv file that is in the include/dataset/202505 directory and save to Parquet file partitioned by 'patch' column. Given information: the "include" directory is inside the root folder of the project and the current file is in rootfolder/dags/elt_pipeline_dags/extract_and_load.py
#     @task
#     def read_and_save_players():
#         spark = SparkSession.builder.appName("Extract and Load Players").getOrCreate()
        
#         # Define the path to the dataset directory
#         dataset_path = os.path.join(os.path.dirname(__file__), '../../include/dataset')
        
#         # Read all players.csv files in subfolders
#         df = spark.read.csv(os.path.join(dataset_path, '**', 'players.csv'), header=True, inferSchema=True, multiLine=True)
        
#         # Save to Parquet file partitioned by 'patch' column
#         output_path = os.path.join(dataset_path, 'output', 'players.parquet')
#         df.write.mode('overwrite').partitionBy('patch').parquet(output_path)
        
#         spark.stop()

from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

@dag(schedule_interval=None, catchup=False)
def extract_and_save():

    read_and_save_players = SparkSubmitOperator(
        task_id='read_and_save_players',
        application='./include/scripts/read_players.py',
        name='Read and Save Players',
        conn_id='my_spark_conn',
        verbose=True,
    )

    read_and_save_players

extract_and_save()