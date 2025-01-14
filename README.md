# BI-Dev-test
This is for prospective BI Developers interviewing at Holman.

# Directions:
1. Fork this repo to your personal Github account.
2. Complete the below data exercises in the best way you see fit. Your project should be shareable with a link prior to your 2nd round interview. Here are some potential implementation options:

- Develop within a PySpark Project: https://github.com/AlexIoannides/pyspark-example-project 
- Use a Jupyter notebook: https://jupyter.org/ or https://colab.google/ 
- Implement a local database: 
    - https://www.prisma.io/dataguide/postgresql/setting-up-a-local-postgresql-database
    - https://www.prisma.io/dataguide/mysql/setting-up-a-local-mysql-database 

## Olympics && Countries
1. Run the `ingest.py` script in the `./tables` directory to have usable parquet files to run analytics on.
2. Create a new python file or notebook, and read the two parquet files as dataframes to begin working.

## Combining Olympics and Countries
Transform one or both of the olympics/countries tables to facilitate a join across these tables. 

1. Normalize the data by applying a foreign key(s) to one/both tables. The key should be unique to represent 1 country, and ensure no cartesian joins occur. (We are aware that no true key exists, and an artificial key(s) will need to be produced)
2. Transform the 2 data objects via denormalization as you see fit to answer the below questions.
   - Who has won the most silver medals across all years of data?
   - Which year did that country win the most total medals?
   - Is there a correlation between Population Density and winning medals?
   - Is there a correlation between GDP and winning gold medals?
  
3. Separate the Olympics and Countries data into a star schema as you would in a gold layer before bringing into a PowerBI semantic model.
4. Pick another two tables from `./datasets/countries_health` and include them in the star schema structure noting what columns would be used for relationships.