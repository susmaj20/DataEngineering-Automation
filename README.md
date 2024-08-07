# DataEngineering-Automation
Author - Susmita Majumdar

This is a ETL project which does the following steps - 
1. It extracts news information from "News API" - 'newsapi.org' based on the keywords / input provided in the Python Code - 'news_fetcher_etl.py'.
2. Then the extracted dataframe is moved to AWS S3 bucket in the form of a parquet file using Airflow BashOperator command.
3. Then a table is created in Snowflake cloud data warehouse using Airflow SnowflakeOperator
4. The data is copied from AWS S3 bucket to Snowflake table using external staging and COPY INTO command in Snowflake.
5. This data can be now used by downstream processes for various analytics purposes.

The entire project is deployed in AWS EC2 instance where necessary packages are installed and the pipeline is triggered on-demand basis by Airflow (to avoid costs by EC2 instance).

Please find the Airflow DAG diagram below - 

<img width="300" alt="image" src="https://github.com/user-attachments/assets/0ce19a94-96ee-4ad8-a9ab-87e6e4574762">


Please find the architecture diagram for this project below - 

<img width="500" alt="image" src="https://github.com/user-attachments/assets/b072f5ff-c18e-47f1-b85c-6e3025ebf5b8">



