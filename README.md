# DataEngineering-Automation
Author - Susmita Majumdar

This is a ETL project which does the following steps - 
1. It extracts news information from "News API" - 'newsapi.org' based on the keywords / input provided in the Python Code - 'news_fetcher_etl.py'.
2. Then the extracted dataframe is moved to AWS S3 bucket in the form of a parquet file using Airflow BashOperator command.
3. Then a table is created in Snowflake cloud data warehouse using Airflow SnowflakeOperator
4. The data is copied from AWS S3 bucket to Snowflake table using external staging and COPY INTO command in Snowflake.
5. This data can be now used by downstream processes for various analytics purposes.

<img width="395" alt="image" src="https://github.com/user-attachments/assets/b072f5ff-c18e-47f1-b85c-6e3025ebf5b8">

