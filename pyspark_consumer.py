import pyspark
from pyspark.sql import SparkSession, types
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import col, from_json, count




pyspark_version = pyspark.__version__
kafka_jar_package = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark_version}"

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("GreenTripsConsumer") \
    .config("spark.jars.packages", kafka_jar_package) \
    .getOrCreate()

green_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "green-trips") \
    .option("startingOffsets", "earliest") \
    .load()

schema = StructType([
    StructField("green-trips", StructType([
        StructField("lpep_pickup_datetime", StringType(), nullable=True),
        StructField("lpep_dropoff_datetime", StringType(), nullable=True),
        StructField("PULocationID", IntegerType(), nullable=True),
        StructField("DOLocationID", IntegerType(), nullable=True),
        StructField("passenger_count", DoubleType(), nullable=True),
        StructField("trip_distance", DoubleType(), nullable=True),
        StructField("tip_amount", DoubleType(), nullable=True)
    ]))
])

# Parse the JSON data using the specified schema
green_stream = green_stream \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*")

green_stream = green_stream.select(
    "green-trips.lpep_pickup_datetime",
    "green-trips.lpep_dropoff_datetime",
    "green-trips.PULocationID",
    "green-trips.DOLocationID",
    "green-trips.passenger_count",
    "green-trips.trip_distance",
    "green-trips.tip_amount"
)

# add column current_timestamp
popular_destinations = green_stream.withColumn("current_timestamp", F.to_timestamp(current_timestamp()))

# Group by the "DOLocationID" column and aggregate using count
# Group by 5-minute window of timestamp and DOLocationID
popular_destinations = popular_destinations.groupBy(
    F.window("current_timestamp", "5 minutes"),
    "DOLocationID").count().orderBy("count", ascending=False)


query = popular_destinations \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
