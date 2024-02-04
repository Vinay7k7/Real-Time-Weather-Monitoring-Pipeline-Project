""" This code is used for pushing the real time data 
        from kafka topic to the File_path 
        In this i am trying to convert the data in to parquet file because
        I thought of using the aws Glue on this file to perform Crawling operations 
        for Future use. In my case ---> ( AWS ) !

        
            🦉 Created By :- Vinay Kumar Karivena.
            🧨 Date       :-04-02-2024.
            🍔 Use        :- For Mini Project in Cognizant

"""


from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

# Initialize Spark session fro real time data change int he file_path🗂️
spark = SparkSession.builder \
    .appName("MongoDBConnectorExample") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# Define Kafka parameters to connect to the local running kafka  servers
kafka_params = {
    "kafka.bootstrap.servers": "localhost:9092",
    "subscribe": "weather"
}

# Define the schema for the incoming data from the file_path !🛒
schema = StructType([
    StructField("coord", StructType([
        StructField("lon", FloatType(), True),
        StructField("lat", FloatType(), True)
    ]), True),
    StructField("weather", StringType(), True),
    StructField("base", StringType(), True),
    StructField("main", StructType([
        StructField("temp", FloatType(), True),
        StructField("feels_like", FloatType(), True),
        StructField("temp_min", FloatType(), True),
        StructField("temp_max", FloatType(), True),
        StructField("pressure", IntegerType(), True),
        StructField("humidity", IntegerType(), True)
    ]), True),
    StructField("visibility", IntegerType(), True),
    StructField("wind", StructType([
        StructField("speed", FloatType(), True),
        StructField("deg", IntegerType(), True)
    ]), True),
    StructField("clouds", StructType([
        StructField("all", IntegerType(), True)
    ]), True),
    StructField("dt", IntegerType(), True),
    StructField("sys", StructType([
        StructField("type", IntegerType(), True),
        StructField("id", IntegerType(), True),
        StructField("country", StringType(), True),
        StructField("sunrise", IntegerType(), True),
        StructField("sunset", IntegerType(), True)
    ]), True),
    StructField("timezone", IntegerType(), True),
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("cod", IntegerType(), True)
])

# Create a DataFrame that represents streaming data from Kafka🎳
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_params["kafka.bootstrap.servers"]) \
    .option("subscribe", kafka_params["subscribe"]) \
    .load()

# Extract the value field from Kafka message and convert it to JSON💎
json_df = df.selectExpr("CAST(value AS STRING)").select(from_json("value", schema).alias("data"))

# Add a timestamp column it is my use !⌛
timestamped_df = json_df.withColumn("timestamp", current_timestamp())

# Flatten the nested structure to make it easier to work with😁
flattened_df = timestamped_df.select("timestamp", "data.*")

# Write streaming data to a temporary location🗂️
temp_location = "/tmp/weather_data_temp"

query = flattened_df \
    .writeStream \
    .format("parquet") \
    .option("path", temp_location) \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()

query.awaitTermination()

# Read from the temporary location and write to MongoDB # this is no use !
# as i am using the other code for this operations !
batch_df = spark.read.parquet(temp_location)

# Write data to MongoDB Atlas
mongo_write_conf = {
    "uri": "mongodb+srv://Vinay:VinayKumarKarivena77:VinayKumarKarivena77@cluster0.dnbkemf.mongodb.net/weather.data"
}

batch_df.write \
    .format("mongo") \
    .options(**mongo_write_conf) \
    .mode("append") \
    .save()

# Stop the Spark session
spark.stop()
