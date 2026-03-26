create or replace database DBP;
select current_database();
use schema public;
create schema stgs;
create schema FF;
create schema streams;
Select current_schema();

--creating File Format
CREATE OR REPLACE FILE FORMAT FF.CSV
TYPE = CSV
SKIP_HEADER = 1
FIELD_DELIMITER = ','
TRIM_SPACE = TRUE
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
NULL_IF = ('NULL','Null','N','NA');
describe file format FF.csv;
create or replace file format FF.CSV_INFER
type = csv
skip_header = 0
parse_header = true;

use schema STGS;

create or replace storage integration s3_int
type = external_stage
storage_provider = S3
enabled  = true
storage_aws_role_arn = 'arn:aws:iam::049706517980:role/start'
storage_allowed_locations = ('s3://sps66/All/');

create or replace storage integration s3_mulpath
type = external_stage
storage_provider = s3
enabled = true
storage_aws_role_arn = 'arn:aws:iam::049706517980:role/start'
storage_allowed_locations = ('s3://mlpath/path1/');
describe storage integration s3_mulpath;

select CURRENT_SCHEMA();
describe storage integration S3_int;

create or replace stage csv_stg
url = 's3://mlpath/path1/'
storage_integration = s3_mulpath;

describe stage csv_stg; 

list @csv_stg;

--Using INFER SCHEMA Create EMP Table
Select * from table(INFER_SCHEMA(location => '@DBP.STGS.CSV_STG' , FILE_FORMAT => 'DBP.FF.CSV_INFER', files => 'emp2.csv'));
--creating the table now
create or replace table dbp.public.emp using template (Select ARRAY_AGG(OBJECT_CONSTRUCT(*)) from table(INFER_SCHEMA(location => '@DBP.STGS.CSV_STG' , FILE_FORMAT => 'DBP.FF.CSV_INFER', files => 'emp2.csv')));
Select * from public.emp;
describe table emp;
--Loading the data from Parent and child directory files 

COPY into DBP.PUBLIC.EMP from @DBP.STGS.csv_stg 
file_format = (FORMAT_NAME = DBP.FF.CSV)
pattern = '.*emp.*\\.csv';



