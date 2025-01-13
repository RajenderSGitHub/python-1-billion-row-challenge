from pyspark.sql.session import SparkSession
from pyspark.sql import functions as F 
import time

start_time = time.time()
spark = SparkSession.builder.appName("Python 1B rows").getOrCreate()

df = spark.read.csv("measurements_small.csv", header=True, inferSchema=True)

df_result = (df.groupBy("name")
                .agg(F.min("temparature").alias("min_temparature"),
                     F.round(F.mean("temparature"),2).alias("avg_temparature"),
                     F.max("temparature").alias("max_temparature")
                     )
            )

print(df_result.show())

# Write the DataFrame to CSV
df_result.write.option("header", "true").csv("output/result_summary_spark_500.csv")

end_time = time.time()

print(f"Time taken to process the records Spark : {(end_time-start_time)*1000} ms")
