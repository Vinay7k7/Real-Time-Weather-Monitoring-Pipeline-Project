""" This code is used to take the real time added data from
        The Path given in my case parquet file location 

        
            🦉 Created By :- Vinay Kumar Karivena.
            🧨 Date       :-04-02-2024.
            🍔 Use        :- For Mini Project in Cognizant

"""


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

# Initialize Spark session for live data flow 🛩️
spark = SparkSession.builder \
    .appName("MongoDBConnectorExample") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# Define the temporary location where streaming data is written🏛️
temp_location = "/tmp/weather_data_temp"

# Define MongoDB Atlas parameters which are 
# "<Server>://<Name>:<Password>@cluster0.dnbkemf.mongodb.net/?retryWrites=true&w=majority"🩳
mongo_uri = "mongodb+srv://Vinay:VinayKumarKarivena@cluster0.dnbkemf.mongodb.net/?retryWrites=true&w=majority"
mongo_write_conf = {
    "uri": mongo_uri,
    "database": "weather",
    "collection": "data",
}

# Define schema for the streaming data that i am getting from the api end_point🛰️
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

# Define streaming DataFrame to continuously read data from the temporary location in real time⌚
streaming_df = spark.readStream \
    .schema(schema) \
    .format("parquet") \
    .option("path", temp_location) \
    .load()

# Write streaming DataFrame to MongoDB Atlas using MongoDB Spark Connector🪢
query = streaming_df.writeStream \
    .outputMode("append") \
    .foreachBatch(lambda batch_df, batch_id: batch_df.write.format("mongo").options(**mongo_write_conf).mode("append").save()) \
    .start()

# Await termination of the streaming query int he loop !
query.awaitTermination()

# Stop the Spark session
spark.stop()
