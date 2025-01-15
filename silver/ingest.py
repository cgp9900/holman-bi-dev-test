from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, split, lit, col, sum as spark_sum, regexp_extract, \
monotonically_increasing_id, concat
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Initialize Spark session
spark = SparkSession.builder.appName("OlympicCountryDataPipeline").getOrCreate()

# Define schemas
olympic_schema = StructType([
    StructField("Country_Code", StringType(), True),
    StructField("Gold", IntegerType(), True),
    StructField("Silver", IntegerType(), True),
    StructField("Bronze", IntegerType(), True),
    StructField("Total", IntegerType(), True)
])

countries_schema = StructType([
    StructField("Country", StringType(), True),
    StructField("Region", StringType(), True),
    StructField("Population", DoubleType(), True),
    StructField("Area_sq_mi", DoubleType(), True),
    StructField("Pop_Density_per_sq_mi", DoubleType(), True),
    StructField("Coastline_coast_area_ratio", DoubleType(), True),
    StructField("Net_migration", DoubleType(), True),
    StructField("Infant_mortality_per_1000_births", DoubleType(), True),
    StructField("GDP_per_capita", DoubleType(), True),
    StructField("Literacy_percent", DoubleType(), True),
    StructField("Phones_per_1000", DoubleType(), True),
    StructField("Arable_percent", DoubleType(), True),
    StructField("Crops_percent", DoubleType(), True),
    StructField("Other_percent", DoubleType(), True),
    StructField("Climate", DoubleType(), True),
    StructField("Birthrate", DoubleType(), True),
    StructField("Deathrate", DoubleType(), True),
    StructField("Agriculture", DoubleType(), True),
    StructField("Industry", DoubleType(), True),
    StructField("Service", DoubleType(), True)
])

# Read All Olympic csv data - added additional splitting to correct year regex split 
# (spaces in file names are rough :))
olympic_df = spark.read.csv("datasets/olympics/*.csv", schema=olympic_schema)
olympic_df = olympic_df.withColumn("Year", split(input_file_name(), "/").cast("string"))
olympic_df = olympic_df.withColumn("Year", split(col("Year"), "%20").cast("string"))
olympic_df = olympic_df.withColumn("Year", regexp_extract(col("Year"), r"(\d{4})", 0))

# Read Countries data
countries_df = spark.read.csv("datasets/countries/countries of the world.csv", header=True, schema=countries_schema)
countries_df = countries_df.withColumnRenamed("Country", "Country_Name")

# Write to Parquet (a queryable format)
olympic_df.write.mode("overwrite").parquet("olympic_combined.parquet")
countries_df.write.mode("overwrite").parquet("countries.parquet")