from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import col, when


spark = (SparkSession.builder.appName("SimpleSparkJob").master("spark://34.142.194.212:7077")
         .config("spark.jars", "/opt/spark/jars/gcs-connector-latest-hadoop2.jar")
         .config("spark.executor.memory", "1G")  #excutor excute only 2G
        .config("spark.driver.memory","1G") 
        .config("spark.executor.cores","1") #Cluster use only 3 cores to excute as it has 3 server
        .config("spark.python.worker.memory","1G") # each worker use 1G to excute
        .config("spark.driver.maxResultSize","1G") #Maximum size of result is 3G
        .config("spark.kryoserializer.buffer.max","1024M")
         .getOrCreate())


spark.conf.set("google.cloud.auth.service.account.json.keyfile","/opt/bucket_connector/lucky-wall-393304-3fbad5f3943c.json")
spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
spark._jsc.hadoopConfiguration().set('fs.gs.auth.service.account.enable', 'true')


path=f"gs://it4043e-it5384/it4043e/it4043e_group19_problem5/transaction_data/arbitrum_blockchain_etl_transaction.csv"
df =spark.read.csv(path, header=True)


user_addresses = df.select("from_address").union(df.select("to_address")).distinct()
user_addresses = user_addresses.withColumnRenamed("from_address", "user_address")


in_degree = df.groupBy("to_address").agg(F.count("_id").alias("user_in_degree"))
out_degree = df.groupBy("from_address").agg(F.count("_id").alias("user_out_degree"))


unique_in_degree = df.groupBy("to_address").agg(F.countDistinct("from_address").alias("user_unique_in_degree"))
unique_out_degree = df.groupBy("from_address").agg(F.countDistinct("to_address").alias("user_unique_out_degree"))


mean_in = df.groupBy("to_address").agg(F.mean("value").alias("user_mean_in"))
mean_out = df.groupBy("from_address").agg(F.mean("value").alias("user_mean_out"))


stddev_in = df.groupBy("to_address").agg(F.stddev("value").alias("user_stddev_in"))
stddev_out = df.groupBy("from_address").agg(F.stddev("value").alias("user_stddev_out"))


timestamp_ratios_in = df.groupBy("to_address").agg(F.count("_id").alias("num_in_transactions"),
                                                    F.countDistinct("block_timestamp").alias("unique_in_timestamps"))
timestamp_ratios_out = df.groupBy("from_address").agg(F.count("_id").alias("num_out_transactions"),
                                                      F.countDistinct("block_timestamp").alias("unique_out_timestamps"))


transaction_rates_in = df.groupBy("to_address").agg((F.count("_id") / F.countDistinct("block_timestamp")).alias("in_transaction_rate"))
transaction_rates_out = df.groupBy("from_address").agg((F.count("_id") / F.countDistinct("block_timestamp")).alias("out_transaction_rate"))


balance = df.groupBy("from_address").agg((F.sum("value") - F.sum(when(col("to_address") != "0", col("value")).otherwise(0))).alias("balance"))


to_address_df = df.select("to_address", "block_timestamp", "value")

window_spec = Window.partitionBy("to_address").orderBy("block_timestamp")
to_address_df = to_address_df.withColumn("timestamp_diff", F.col("block_timestamp") - F.lag("block_timestamp").over(window_spec))

total_value_per_address = to_address_df.groupBy("to_address").agg(F.sum("value").alias("total_value"))


in_velocity = (
    total_value_per_address
    .join(to_address_df.groupBy("to_address").agg(F.max("timestamp_diff").alias("max_timestamp_diff")), "to_address", "inner")
    .withColumn("in_velocity", F.col("total_value") / F.col("max_timestamp_diff"))
    .select("to_address", "in_velocity")
)


in_acceleration = (
    in_velocity
    .join(to_address_df.groupBy("to_address").agg(F.max("timestamp_diff").alias("max_timestamp_diff")), "to_address", "inner")
    .withColumn("in_acceleration", F.col("in_velocity") / F.col("max_timestamp_diff"))
    .select("to_address", "in_acceleration")
)


from_address_df = df.select("from_address", "block_timestamp", "value")

window_spec = Window.partitionBy("from_address").orderBy("block_timestamp")
from_address_df = from_address_df.withColumn("timestamp_diff", F.col("block_timestamp") - F.lag("block_timestamp").over(window_spec))

total_value_per_address = from_address_df.groupBy("from_address").agg(F.sum("value").alias("total_value"))


out_velocity = (
    total_value_per_address
    .join(from_address_df.groupBy("from_address").agg(F.max("timestamp_diff").alias("max_timestamp_diff")), "from_address", "inner")
    .withColumn("out_velocity", F.col("total_value") / F.col("max_timestamp_diff"))
    .select("from_address", "out_velocity")
)


out_acceleration = (
    out_velocity
    .join(from_address_df.groupBy("from_address").agg(F.max("timestamp_diff").alias("max_timestamp_diff")), "from_address", "inner")
    .withColumn("out_acceleration", F.col("out_velocity") / F.col("max_timestamp_diff"))
    .select("from_address", "out_acceleration")
)



ftm_transform_df = user_addresses \
    .join(in_degree, user_addresses["user_address"] == in_degree["to_address"], "left_outer") \
    .join(out_degree, user_addresses["user_address"] == out_degree["from_address"], "left_outer") \
    .join(unique_in_degree, user_addresses["user_address"] == unique_in_degree["to_address"], "left_outer") \
    .join(unique_out_degree, user_addresses["user_address"] == unique_out_degree["from_address"], "left_outer") \
    .join(mean_in, user_addresses["user_address"] == mean_in["to_address"], "left_outer") \
    .join(mean_out, user_addresses["user_address"] == mean_out["from_address"], "left_outer") \
    .join(stddev_in, user_addresses["user_address"] == stddev_in["to_address"], "left_outer") \
    .join(stddev_out, user_addresses["user_address"] == stddev_out["from_address"], "left_outer") \
    .join(timestamp_ratios_in, user_addresses["user_address"] == timestamp_ratios_in["to_address"], "left_outer") \
    .join(timestamp_ratios_out, user_addresses["user_address"] == timestamp_ratios_out["from_address"], "left_outer") \
    .join(transaction_rates_in, user_addresses["user_address"] == transaction_rates_in["to_address"], "left_outer") \
    .join(transaction_rates_out, user_addresses["user_address"] == transaction_rates_out["from_address"], "left_outer") \
    .join(balance, user_addresses["user_address"] == balance["from_address"], "left_outer") \
    .join(in_velocity, user_addresses["user_address"] == in_velocity["to_address"], "left_outer") \
    .join(out_velocity, user_addresses["user_address"] == out_velocity["from_address"], "left_outer") \
    .join(in_acceleration, user_addresses["user_address"] == in_acceleration["to_address"], "left_outer") \
    .join(out_acceleration, user_addresses["user_address"] == out_acceleration["from_address"], "left_outer") \
    .select("user_address",
            "user_in_degree", "user_out_degree",
            "user_unique_in_degree", "user_unique_out_degree",
            "user_mean_in", "user_mean_out",
            "user_stddev_in", "user_stddev_out",
            (col("num_in_transactions") / col("unique_in_timestamps")).alias("num_in_transactions_per_timestamp"),
            (col("num_out_transactions") / col("unique_out_timestamps")).alias("num_out_transactions_per_timestamp"),
            "in_transaction_rate", "out_transaction_rate",
            "balance",
            "in_velocity", "out_velocity",
            "in_acceleration", "out_acceleration")


ftm_transform_df = ftm_transform_df.na.fill(value=0, subset=["user_in_degree", "user_out_degree",
            "user_unique_in_degree", "user_unique_out_degree"])
ftm_transform_df = ftm_transform_df.na.fill(value=0.0, subset=["user_mean_in", "user_mean_out",
            "user_stddev_in", "user_stddev_out", "num_in_transactions_per_timestamp",
            "num_out_transactions_per_timestamp", "in_transaction_rate", "out_transaction_rate",
            "balance", "in_velocity", "out_velocity",
            "in_acceleration", "out_acceleration"])



temp_path = f"gs://it4043e-it5384/it4043e/it4043e_group19_problem5/processed_data/__temp"
target_path = f"gs://it4043e-it5384/it4043e/it4043e_group19_problem5/processed_data/process_arbi_transaction_final.csv"
# ftm1 = ftm_transform_df.copy()
ftm_transform_df.coalesce(1).write.mode("overwrite").csv(temp_path)
ftm1 = ftm_transform_df.copy()
Path = sc._gateway.jvm.org.apache.hadoop.fs.Path

fs = Path(temp_path).getFileSystem(sc._jsc.hadoopConfiguration())
csv_part_file = fs.globStatus(Path(temp_path + "/part*"))[0].getPath()

fs.rename(csv_part_file, Path(target_path))
# fs.delete(Path(temp_path), True)

spark.stop()



