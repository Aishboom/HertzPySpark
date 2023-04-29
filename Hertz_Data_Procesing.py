from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, col, to_date, explode, lit
from pyspark.sql.types import (
    StructType,
    StringType,
    ArrayType,
    IntegerType,
    DoubleType,
    StructField, DateType,
)

# create a SparkSession
spark = SparkSession.builder.appName("Big Data Processing JSON Files").getOrCreate()

# specify the path to the folder containing JSON files
folder_path = "C:/Users/dakpr/Downloads/events/events/"
output_path = "C:/Users/dakpr/Downloads/events/outputs1/"

# define schema
new_schema = StructType(
    [
        StructField("timestamp", StringType(), True),
        StructField("type", StringType(), True),
        StructField("subtype", StringType(), True),
        StructField("eventId", StringType(), True),
        StructField(
            "eventData",
            StructType(
                [
                    StructField("latitude", StringType(), True),
                    StructField("longitude", StringType(), True),
                    StructField("sensor", StringType(), True),
                    StructField(
                        "sensorData",
                        StructType(
                            [
                                StructField("mileage", DoubleType(), True),
                                StructField("fuelLevel", DoubleType(), True),
                                StructField("fuelAmount", StringType(), True),
                                StructField("batteryLevel", StringType(), True),
                                StructField("remainingLife", StringType(), True),
                                StructField(
                                    "wheels",
                                    ArrayType(
                                        StructType(
                                            [
                                                StructField("axle", IntegerType(), True),
                                                StructField("side", StringType(), True),
                                                StructField("position", StringType(), True),
                                                StructField("pressure", DoubleType(), True),
                                                StructField("uom", StringType(), True),
                                            ]
                                        )
                                    ),
                                ),
                            ]
                        ), True, ), ]), True, ),
        StructField("vehicleId", StringType(), True),
    ]
)
# read all JSON files from the folder
df = spark.read.format("json").schema(new_schema).load(folder_path)

# Add filename column to dataframe
df = df.withColumn("filename", input_file_name())

# df.select('eventData.sensor').show(truncate=False)

df = df.select(
    col("timestamp"),
    col("type"),
    col("subtype"),
    col("eventId"),
    col("eventData.latitude").alias("latitude"),
    col("eventData.longitude").alias("longitude"),
    col("eventData.sensor").alias("sensor"),
    col("eventData.sensorData.mileage").alias("mileage"),
    col("eventData.sensorData.fuelLevel").alias("fuelLevel"),
    col("eventData.sensorData.fuelAmount").alias("fuelAmount"),
    col("eventData.sensorData.batteryLevel").alias("batteryLevel"),
    col("eventData.sensorData.remainingLife").alias("remainingLife"),
    col("eventData.sensorData.wheels").alias("wheels"),
    col("vehicleId"),
    to_date(col("timestamp")).alias("date"),
)

# print schema
# print(df.printSchema())

# print number of records in dataframes
df.show(truncate=False)

location_events_df = df.select(
    col("timestamp"),
    col("subtype"),
    col("eventId"),
    col("latitude"),
    col("longitude"),
    col("vehicleId"),
    col("date"),
).filter(col("type") == "LOCATION")
diagnostic_events_df = (
    df.select(
        col("timestamp"),
        col("subtype"),
        col("eventId"),
        col("sensor"),
        col("mileage"),
        col("fuelLevel"),
        col("fuelAmount"),
        col("batteryLevel"),
        col("remainingLife"),
        explode(col("wheels")).alias("wheels"),
        col("vehicleId"),
        col("date"),
    ).select(
        "timestamp",
        "subtype",
        "eventId",
        "sensor",
        "mileage",
        "fuelLevel",
        "fuelAmount",
        "batteryLevel",
        "remainingLife",
        "wheels.axle",
        "wheels.side",
        "wheels.position",
        "wheels.pressure",
        "wheels.uom",
        "vehicleId",
        "date",
    ).filter(col("type") == "DIAGNOSTIC")
)

location_events_schema = StructType(
    [
            StructField("timestamp", StringType(), True),
            StructField("subtype", StringType(), True),
            StructField("eventId", StringType(), True),
            StructField("latitude", StringType(), True),
            StructField("longitude", StringType(), True),
            StructField("vehicleId", StringType(), True),
            StructField("date", DateType(), True)
    ]
)

numBuckets = 2

location_events_df \
    .repartition(numBuckets, "eventId") \
    .write \
    .format("parquet") \
    .partitionBy("date") \
    .mode("overwrite") \
    .bucketBy(numBuckets, "eventId") \
    .option("compression", "snappy") \
    .option("mergeSchema", "true") \
    .option("schema", location_events_schema)\
    .option("path", output_path + "location_events_output") \
    .saveAsTable("location_events")

# location_events_df\
#   .write\
#   .format("parquet")\
#   .partitionBy("date")\
#   .mode("overwrite")\
#   .bucketBy(numBuckets, "eventId")\
#   .option("compression", "snappy")\
#   .option("mergeSchema", "true")\
#   .option("path", output_path + "location_events_output")\
#   .saveAsTable("location_events")

# location_events_df\
#   .write\
#   .format("parquet")\
#   .partitionBy("date")\
#   .mode("overwrite")\
#   .option("compression", "snappy")\
#   .option("mergeSchema", "true")\
#   .save(output_path + "location_events_output")

diagnostic_events_schema = StructType(
    [
            StructField("timestamp", StringType(), True),
            StructField("subtype", StringType(), True),
            StructField("eventId", StringType(), True),
            StructField("sensor", StringType(), True),
            StructField("mileage", DoubleType(), True),
            StructField("fuelLevel", DoubleType(), True),
            StructField("fuelAmount", StringType(), True),
            StructField("batteryLevel", StringType(), True),
            StructField("remainingLife", StringType(), True),
            StructField("axle", IntegerType(), True),
            StructField("side", StringType(), True),
            StructField("position", StringType(), True),
            StructField("pressure", DoubleType(), True),
            StructField("uom", StringType(), True),
            StructField("vehicleId", StringType(), True),
            StructField("date", DateType(), True)
    ]
)

diagnostic_events_df \
    .repartition(numBuckets, "eventId") \
    .write \
    .format("parquet") \
    .partitionBy("date") \
    .mode("overwrite") \
    .bucketBy(numBuckets, "eventId") \
    .option("compression", "snappy") \
    .option("mergeSchema", "true") \
    .option("schema", diagnostic_events_schema)\
    .option("path", output_path + "diagnostic_events_output") \
    .saveAsTable("diagnostic_events")

# diagnostic_events_df\
#   .write\
#   .format("parquet")\
#   .partitionBy("date")\
#   .mode("overwrite")\
#   .bucketBy(numBuckets, "eventId")\
#   .option("compression", "snappy")\
#   .option("mergeSchema", "true")\
#   .option("path", output_path + "diagnostic_events_output")\
#   .saveAsTable("diagnostic_events")

# diagnostic_events_df\
#   .write\
#   .format("parquet")\
#   .partitionBy("date")\
#   .mode("overwrite")\
#   .option("compression", "snappy")\
#   .option("mergeSchema", "true")\
#   .save(output_path + "diagnostic_events_output")