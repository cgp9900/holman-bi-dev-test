from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import count_distinct, count, col, trim, lower, monotonically_increasing_id, row_number, \
    when
from pyspark.sql.window import Window
from fuzzywuzzy import process

def ingest_parquet(input_path, spark):
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

def create_country_ids(df_countries):
    """
    This function takes a DataFrame containing country information and creates a new 
    column called 'Country_ID', which assigns a unique sequential ID to each country 
    based on alphabetical order of the 'Country_Name' column.

    :param df_countries: Country-related DataFrame containing a Country_Name column.
    :return: DataFrame containing the Country_ID.
    """

    # Create a window specification to assign row numbers based on Country_Name
    window_spec_countries = Window.orderBy("Country_Name")
    
    # Add Country_ID to df_countries
    df_countries_id = (df_countries
                       .select("Country_Name")
                       .orderBy("Country_Name")
                       .withColumn("Country_ID", row_number().over(window_spec_countries))
                       )
    
    return df_countries_id

def create_country_code_ids(df_olympics):
    """
    This function takes a DataFrame containing olympics information and creates a new 
    column called 'Country_Code_ID', which assigns a unique sequential ID to each country 
    code based on alphabetical order of the 'Country_Code' column.

    :param df_olympics: Country-related DataFrame containing a Country_Code column.
    :return: DataFrame containing the Country_Code_ID.
    """

    # Create a window specification to assign row numbers based on Country_Code
    window_spec_codes = Window.orderBy("Country_Code")
    
    # Add Country_Code_ID to df_olympics
    df_olympics_id = (df_olympics
                      .select("Country_Code")
                      .distinct()
                      .orderBy("Country_Code")
                      .withColumn("Country_Code_ID", row_number().over(window_spec_codes))
                      )
    
    return df_olympics_id

def join_country_and_olympics(df_countries, df_olympics):
    """
    This function joins two DataFrames: one containing country data and the other containing
    Olympic data. It first creates unique identifiers for both DataFrames (Country_ID and 
    Country_Code_ID), then performs two joins to merge the DataFrames based on these IDs. 
    The final DataFrame includes both country and Olympics-related information, with 
    associated IDs for each country.
    
    :param df_countries: Country-related DataFrame containing a Country_Name column.
    :param df_olympics: Country-related DataFrame containing a Country_Code column.
    :return: A DataFrame that contains data from both input DataFrames (`df_countries` 
        and `df_olympics`) merged on their respective country identifiers (Country_ID 
        and Country_Code_ID)
    """

    # Create IDs for both DataFrames
    df_countries_id = create_country_ids(df_countries)
    df_olympics_id = create_country_code_ids(df_olympics)
    
    # Join the IDs to the original df_countries and df_olympics
    df_countries_final = df_countries.join(df_countries_id, on="Country_Name", how="left")
    df_olympics_final = df_olympics.join(df_olympics_id, on="Country_Code", how="left")
    
    # Join the two DataFrames based on the IDs. Left join ensures all olympics data
    # stays in the final table even if there are no corresponding country attributes.
    # This could help us catch errors in our two ID functions. 
    df_countries_olympics = df_olympics_final.join(
        df_countries_final,
        df_olympics_final["Country_Code_ID"] == df_countries_final["Country_ID"],
        how="left"
    )
    
    return df_countries_olympics

def fuzzy_match(spark, df_countries, df_olympics):
    """ 
    This function performs a fuzzy matching process to align country codes from the 
    Olympics DataFrame (`df_olympics`) to country names in the Countries DataFrame 
    (`df_countries`). 

    The process works by:
    1. Extracting distinct country codes from `df_olympics`.
    2. Extracting distinct country names from `df_countries`.
    3. For each country code, finding the most similar country name using fuzzy matching.
    4. Merging the country codes with their matched country names into a lookup DataFrame.
    5. Joining the lookup DataFrame with both `df_olympics` and `df_countries` to align country 
        codes and names.

    :param spark: Existing Spark session.
    :param df_countries: Country-related DataFrame containing a Country_Name column.
    :param df_olympics: Country-related DataFrame containing a Country_Code column.
    :return: A DataFrame that contains data from both input DataFrames (`df_countries` 
        and `df_olympics`) merged through above process 
    """

    # Isolate list of country_codes
    country_codes = (df_olympics
                 .select("Country_Code")
                 .distinct()
                 .orderBy("Country_Code")
                 .rdd.map(lambda x: x[0]).collect()
                 )

    # Isolate list of country names 
    country_names = (df_countries
                    .select("Country_Name")
                    .distinct()
                    .orderBy("Country_Name")
                    .rdd.map(lambda x: x[0]).collect()
                    )

    matches = [] # A list to add the matched code-country pairs to
    matched_countries = set()  # A set to keep track of matched country names

    for code in country_codes:
        # Filter out the already matched countries
        remaining_countries = [name for name in country_names if name not in matched_countries]
        
        # Find the best match from the remaining countries
        best_match = process.extractOne(code, remaining_countries)
        
        # If a best match is found, append it to the list of matches
        if best_match:
            matched_countries.add(best_match[0])  # Add this matched country to the set
            matches.append((code, best_match[0]))  # (Country Code, Best Match Country Name)

    df_lookup = spark.createDataFrame(matches, ["Country_Code_Lookup", "Country_Name_Lookup"])

    # Alias each DataFrame for clarity
    df_countries_aliased = df_countries.alias('co')
    df_olympics_aliased = df_olympics.alias('ol')
    df_lookup_aliased = df_lookup.alias('look')

    # Using a left join in case no fuzzy matching result was found. 
    df_countries_olympics = (df_olympics_aliased
                            .join(df_lookup_aliased, on = col("ol.Country_Code") == col("look.Country_Code_lookup"), how = "left")
                            .join(df_countries_aliased, on = col("look.Country_Name_lookup") == col("co.Country_Name"), how = "left")
                            ).drop("Country_Code_Lookup", "Country_Name_Lookup")
                            
    return df_countries_olympics
