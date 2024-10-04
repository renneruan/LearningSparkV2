from pyspark.sql.types import *

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = (
    SparkSession.builder.appName("FireCalls")
    .config("spark.hadoop.native.lib", "false")
    .getOrCreate()
)

# Programmatic way to define a schema
fire_schema = StructType(
    [
        StructField("CallNumber", IntegerType(), True),
        StructField("UnitID", StringType(), True),
        StructField("IncidentNumber", IntegerType(), True),
        StructField("CallType", StringType(), True),
        StructField("CallDate", StringType(), True),
        StructField("WatchDate", StringType(), True),
        StructField("CallFinalDisposition", StringType(), True),
        StructField("AvailableDtTm", StringType(), True),
        StructField("Address", StringType(), True),
        StructField("City", StringType(), True),
        StructField("Zipcode", IntegerType(), True),
        StructField("Battalion", StringType(), True),
        StructField("StationArea", StringType(), True),
        StructField("Box", StringType(), True),
        StructField("OriginalPriority", StringType(), True),
        StructField("Priority", StringType(), True),
        StructField("FinalPriority", IntegerType(), True),
        StructField("ALSUnit", BooleanType(), True),
        StructField("CallTypeGroup", StringType(), True),
        StructField("NumAlarms", IntegerType(), True),
        StructField("UnitType", StringType(), True),
        StructField("UnitSequenceInCallDispatch", IntegerType(), True),
        StructField("FirePreventionDistrict", StringType(), True),
        StructField("SupervisorDistrict", StringType(), True),
        StructField("Neighborhood", StringType(), True),
        StructField("Location", StringType(), True),
        StructField("RowID", StringType(), True),
        StructField("Delay", FloatType(), True),
    ]
)

sf_fire_file = "chapter3/data/sf-fire-calls.csv"
fire_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)

# fire_df.write.format("parquet").save("chapter3/data/saved/")

# Aspects of the dataset
few_fire_df = fire_df.select("IncidentNumber", "AvailableDtTm", "CallType").where(
    col("CallType") != "Medical Incident"
)
few_fire_df.show(5, truncate=False)

# How many distinct calltype
(
    fire_df.select("CallType")
    .where(col("CallType").isNotNull())
    .agg(countDistinct("CallType").alias("DistinctCallTypes"))
    .show()
)

# List the distinct call type
(
    fire_df.select("CallType")
    .where(col("CallType").isNotNull())
    .distinct()
    .show(10, False)
)

# Renaming columns
new_fire_df = fire_df.withColumnRenamed("Delay", "ResponseDelayedinMins")
(
    new_fire_df.select("ResponseDelayedinMins")
    .where(col("ResponseDelayedinMins") > 5)
    .show(5, False)
)

# Date transformation
fire_ts_df = (
    new_fire_df.withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
    .drop("CallDate")
    .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
    .drop("WatchDate")
    .withColumn(
        "AvailableDtTS", to_timestamp(col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a")
    )
    .drop("AvailableDtTm")
)

# select converted columns
(fire_ts_df.select("IncidentDate", "OnWatchDate", "AvailableDtTS").show(5, False))

# Select distinct years
(
    fire_ts_df.select(year("IncidentDate"))
    .distinct()
    .orderBy(year("IncidentDate"))
    .show()
)

# Most common call type
(
    fire_ts_df.select("CallType")
    .where(col("CallType").isNotNull())
    .groupBy("CallType")
    .count()
    .orderBy("count", ascending=False)
    .show(n=10, truncate=False)
)

# Functions presented on sql.functions
(
    fire_ts_df.select(
        sum("NumAlarms"),
        avg("ResponseDelayedinMins"),
        min("ResponseDelayedinMins"),
        max("ResponseDelayedinMins"),
    ).show()
)


print(fire_df.count())
