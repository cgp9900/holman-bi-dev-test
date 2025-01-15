from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

def ingest_parquet(input_path: str, spark: SparkSession) -> DataFrame:
    """
    Ingest a Parquet file and return it as a DataFrame.

    :param input_path: Path to the input Parquet file.
    :param spark: Existing Spark session.
    :return: DataFrame containing the data from the Parquet file.
    """

    # Read the Parquet file into a DataFrame
    df = (spark
          .read
          .parquet(input_path)
          )

    # Return the DataFrame
    return df