# real-estate-etl
Example for Real Estate ETL with Python and AWS stack (S3, Lambda, CloudWatch).

AWS Lamda function is used to perform ETL on raw json data which gets triggered by new file upload on S3 bucket.
The sample data is for the flats in Berlin available for rent at immobilienscout24.de. 


#### Setup:

1. Deployement:
    - Create AWS Lamda deployement zip file by adding the site packages and source code files at root of the zip file.
    - Create new virtual environment and install packages from requirements.txt file.
    - Upload the zip file to S3 bucket.
4. RDS Database:
    - Create RDS Postgres instance with real_estate database on AWS.
    - Run script db_script.sql on RDS instance.
6. Create S3 Buckets for putting source data files. 
7. AWS Lamda:
    - Create AWS Lamda function and add trigger for new object creation in S3 data bucket.
    - Create and assign policy for AWS Lamda function that will have access to the CloudWatch and S3 Buckets. 
    - Upload the function code zip file from S3 bucket.
    - Set environment variables (DATABASE, DBUSER, DBPASSWORD, ENDPOINT, PORT) for database connection.

#### Test:
1. Upload the raw json data file ending with *yyyymmdd_hh.json format to S3 data bucket.
2. Go to AWS CloudWatch to monitor the AWS Lamda function logs for ETL Processing.
3. Verify the data in tables: 
    - src.real_estate_immobilienscout24
    - dwh.dim_geography
    - dwh.dim_agency
    - dwh.fact_flat
    
#### Note:
- ETL process assumes that the raw json file is uploaded every hour with file name format *yyyymmdd_hh.json

     
#### TODO:
1. Create seprate database users for etl and reporting purpose with required permissions.
2. Create view for masking the PII data from dwh.dim_agency table.
3. Grant the required permissions on masked views and tables without PII data to reporting user.
4. Create and populate the Unknown values in dwh.DimGeography table from the master static data.
5. Modify the logic for loading the data in dwh.fact_flat as per the source data provided. Current ETL process assumes that the hourly raw json file has the data for all flats that is either created or modified. 
If the source data is incremental then better way can be to add a new row for every change with effecitve date key, expired date key and is current flag.
6. Add support for input raw json files to be uploaded in zip format.
7. For better security create VPC, Subnets, security groups and IAM Roles on AWS specific to this application.   
        