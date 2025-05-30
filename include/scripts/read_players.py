from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("Read Players").getOrCreate()

    df = spark.read.csv("./include/dataset/202505.csv", header=True)
    df.show(5)
    # df = df.limit(1000) # Limit to first 1000 rows for testing
    
    # Ensure 'patch' column exists in the DataFrame
    if 'patch' not in df.columns:
        raise ValueError("The 'patch' column is required for partitioning but is not present in the DataFrame.")

    # Ensure the ./include/output directory exists
    import os
    output_dir = "./include"
    # if not os.path.exists(output_dir):
    #     os.makedirs(output_dir)

    # Save the DataFrame to a Parquet file partitioned by 'patch' column
    output_path = os.path.join(output_dir, "output")
    os.makedirs(output_path, exist_ok=True)
    os.chmod(output_path, 0o777)  # full access
    df.write.mode("overwrite").partitionBy("patch").parquet(output_path)
    print(f"Data saved to {output_path} successfully.")   
    
    # Stop the Spark session
    spark.stop()
    
if __name__ == "__main__":
    main()