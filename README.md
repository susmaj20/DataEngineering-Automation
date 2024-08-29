# DataEngineering-Automation
Author - Susmita Majumdar
<img width="700" alt="image" src="https://github.com/user-attachments/assets/a6d9b6f3-03b4-4fa5-8370-0a54338c9296">

AIRFLOW DAG IN AWS EC2:

<img width="700" alt="image" src="https://github.com/user-attachments/assets/b74497eb-b070-4013-a074-4ad7190f9131">


This is a ETL project which does the following steps - 
1. 'extract_redfin_data' extracts real-estate US city-wise house sales data from 'https://www.redfin.com/news/data-center/'. 
2. 'transform_redfin_data' applies python transformations on the raw data and put the csv data in transformed S3 bucket.
3. 'load_to_s3' uses AWS bash command to move the raw data from EC2 /home/ubuntu/ to raw S3 bucket.
4. As soon as transformed csv is created in transformed S3 bucket, a SQS notification triggers Snowpipe object to push the transformed data from external stage to Snowflake 
   table "REDFIN_DATABASE_1"."REDFIN_SCHEMA".""REDFIN_TABLE".
   
   <img width="700" alt="image" src="https://github.com/user-attachments/assets/a3ef8c3b-836b-4d18-836d-6e9262a4c6bc">

6. The final table "REDFIN_DATABASE_1"."REDFIN_SCHEMA".""REDFIN_TABLE_FINAL" is created and used for visualisation in Power BI.

   <img width="1000" alt="image" src="https://github.com/user-attachments/assets/460ec64e-2faa-4bd8-bac5-a45b79869768">








