from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, col, to_date, explode, lit, date_format, hour
from pyspark.sql.types import (
    StructType,
    StringType,
    ArrayType,
    IntegerType,
    DoubleType,
    StructField, DateType,
)

def car_diagonastic():
    # create a SparkSession
    spark = SparkSession.builder.appName("Big Data Processing JSON Files")\
        .config("spark.sql.warehouse.dir", "file:/C:/tempwarhouse/") \
        .config("spark.sql.shuffle.partitions", 100)\
        .getOrCreate()

    # specify the path to the folder containing JSON files
    folder_path = "events/"
    output_path = "file:/C:/tempwarhouse/"

    # define schema for the JSON files
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

    # read all JSON files from the folder with the defined schema
    df = spark.read.format("json").schema(new_schema).load(folder_path)

    # perform repartition based on the number of executor.instance * executor.cores
    df = df.repartition("type")

    # add a filename column to dataframe to check which filename has the schema mismatch
    #df = df.withColumn("filename", input_file_name())

    # select relevant columns and rename some for readability
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
        hour("timestamp").alias("Hour"),
        col("filename")
    )

    # print schema and number of records in dataframe
    #df.show(truncate=False)

    # separate location events and diagnostic events into two dataframes

    location_events_df = df.select(
        col("timestamp"),
        col("subtype"),
        col("eventId"),
        col("latitude"),
        col("longitude"),
        col("vehicleId"),
        col("date"),
        col("Hour")
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
            col("Hour")
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
            "Hour"
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
                StructField("date", DateType(), True),
                StructField("Hour", IntegerType(), True)
        ]
    )

    # fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    # fs.mkdirs(spark._jvm.org.apache.hadoop.fs.Path(output_path + "location_events_output"))

    numBuckets = 2

    location_events_df \
        .repartition(numBuckets, "eventId") \
        .write \
        .format("parquet") \
        .partitionBy("date","Hour") \
        .mode("overwrite") \
        .bucketBy(numBuckets, "eventId") \
        .option("compression", "snappy") \
        .option("mergeSchema", "true") \
        .option("schema", location_events_schema)\
        .option("path", output_path + "location_events_output")\
        .saveAsTable("location_events")


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
                StructField("date", DateType(), True),
                StructField("Hour", IntegerType(), True)
        ]
    )
    # fs.mkdirs(spark._jvm.org.apache.hadoop.fs.Path(output_path + "diagnostic_events_output"))
    diagnostic_events_df \
        .repartition(numBuckets, "eventId") \
        .write \
        .format("parquet") \
        .partitionBy("date", "Hour") \
        .mode("overwrite") \
        .bucketBy(numBuckets, "eventId") \
        .option("compression", "snappy") \
        .option("mergeSchema", "true") \
        .option("schema", diagnostic_events_schema)\
        .option("path", output_path + "diagnostic_events_output")\
        .saveAsTable("diagnostic_events")

if __name__=="__main__":
    car_diagonastic()