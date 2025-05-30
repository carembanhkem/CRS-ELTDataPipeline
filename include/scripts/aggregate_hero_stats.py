from pyspark.sql import SparkSession

def sum_nested_values(d):
    total = 0
    for v in d.values():
        if isinstance(v, dict):
            total += sum_nested_values(v)
        elif isinstance(v, (int, float)):
            total += v
    return total

def main():
    spark = SparkSession.builder.appName("Read Players").getOrCreate()

    df = spark.read.csv("./include/dataset/202505.csv", header=True)
    df.show(5)
    # Add some basic transformations
    from pyspark.sql import functions as F
    from pyspark.sql.types import IntegerType

    def safe_sum_nested_values(x):
        try:
            return sum_nested_values(eval(x)) if isinstance(x, str) else 0
        except Exception:
            return 0

    sum_nested_udf = F.udf(safe_sum_nested_values, IntegerType())

    df = df.withColumn("damage_targets", sum_nested_udf(F.col("damage_targets")))
    df = df.withColumn("healing", sum_nested_udf(F.col("healing")))

    # Create some new columns based on existing ones
    # column df["last_hits_per_min"] = df["last_hits"] / (df["duration"]/60)
    # column df["hero_damage_per_min"] = df["damage_targets"] / (df["duration"]/60)
    # column df["last_hits_per_min"] = df["healing"] / (df["duration"]/60)
    df = df.withColumn("last_hits_per_min", df["last_hits"] / (df["duration"]/60))
    df = df.withColumn("hero_damage_per_min", df["damage_targets"] / (df["duration"]/60))
    df = df.withColumn("healing_per_min", df["healing"] / (df["duration"]/60))
    df.show(5)

    # Group by "hero_id" and "patch" then calculate the average of gold_per_min,xp_per_min,kills_per_min,last_hits_per_min,hero_damage_per_min,hero_healing_per_min,tower_damage. Given df is a pyspark dataframe
    from pyspark.sql import functions as F
    df = df.groupBy("hero_id", "patch").agg(
        F.avg("gold_per_min").alias("avg_gold_per_min"),
        F.avg("xp_per_min").alias("avg_xp_per_min"),
        F.avg("kills_per_min").alias("avg_kills_per_min"),
        F.avg("last_hits_per_min").alias("avg_last_hits_per_min"),
        F.avg("hero_damage_per_min").alias("avg_hero_damage_per_min"),
        F.avg("healing_per_min").alias("avg_healing_per_min"),
        F.avg("tower_damage").alias("avg_tower_damage")
    )
    # Keep only the relevant columns
    df = df.select("hero_id", "patch", 
                     "avg_gold_per_min", 
                     "avg_xp_per_min", 
                     "avg_kills_per_min", 
                     "avg_last_hits_per_min", 
                     "avg_hero_damage_per_min", 
                     "avg_healing_per_min", 
                     "avg_tower_damage")
    df.show(5)
    
    # Stop the Spark session
    spark.stop()
    
if __name__ == "__main__":
    main()