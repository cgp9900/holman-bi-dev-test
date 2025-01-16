# Cole Parker Comments
Here are a few key notes about my project!

### Structure
I mostly utilized notebooks for their readability. In many cases, a .py file within the etl folder would make more sense for a deployed, production workflow, where I would have put the silver folder as a subfolder.
- **etl** folder: contains a read and normalization script, which houses several functions for ingesting the parquet files and normalizing the olympics and countries datasets. 
- **silver** folder: adjusted the `ingest.py` file.
- **notebooks** folder: This houses the execution of the provided directions. 
  - `01_read_normalize.ipynb`: Ingests parquet and executes two methods of joining the data. One using a more traditional integer ID assignment, and the other using fuzzy text matching (text data is fun so this was an enjoyable challenge)
  - `02_analytics_queries.ipynb`: (Great name I know) Answers the four questions below
  - `03_dimensional_modeling.ipynb`: Takes the joined olympics and countries data sets and splits them out into dimension and fact tables. Creates two additional dimension tables using the countries health data

### General Notes
1. I had to adjust the `ingest.py` script for the following: some of the data-types in the declared schema conflicted with the formatting in the csv files. Additionally, the use of `split()` and `input_file_name()` had to be adjusted due to spaces present in the file names. Prior to these adjustments, several columns in the countries dataset were NULL, and the Year column in the olympics dataset was NULL. No worries though, regex work is fun :)
2. As stated in the provided instructions, I am assuming there is no accurate way to join country names and country codes (without manually creating a dictionary or list of tuples), as names do not follow a rules based logic for generating their codes. I'd guess external API's could handle this as well, assuming the country names follow ISO standards. 
3. If my solutions were developed in a more scalable way, the first change would be the gold modeling portion. Similar code as is in my third notebook could be housed in functions that take in parameters specifying dimension/fact, ID columns, dimensional attributes, and more.
4. I followed a very basic branching strategy for my project: main -> dev -> feature. In reality, I would have many more feature branches throughout development to ensure code changes are completely relevant to their corresponding feature branch (and could then be deleted after merging). I'd be curious to know Holman's branching strategy so I can familiarize myself with that workflow!

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
