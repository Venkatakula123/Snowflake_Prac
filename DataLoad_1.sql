use database DBP;
use schema public;  
CREATE OR REPLACE TABLE DBP.PUBLIC.LOAN_PAYMENT (
  "Loan_ID" STRING,
  "loan_status" STRING,
  "Principal" STRING,
  "terms" STRING,
  "effective_date" STRING,
  "due_date" STRING,
  "paid_off_time" STRING,
  "past_due_days" STRING,
  "age" STRING,
  "education" STRING,
  "Gender" STRING
 );

 Select * from LOAN_PAYMENT;

 COPY INTO PUBLIC.LOAN_PAYMENT
    FROM s3://bucketsnowflakes3/Loan_payments_data.csv
    file_format = (type = csv , field_delimiter = ',' , skip_header=1);    -- So this hard coding the Location & that too for the public s3 bucket also called as Named Stage

-- TO avoid this hard coding we need to go for creating the External stages. By using this external stages we can list down the Files as well
show schemas;
use schema stgs;

create or replace stage s3_1
url = 's3://bucketsnowflakes3';

Select current_schema();

List @s3_1;

--We can query the data in the files like below by using $ 
Select $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13 from @s3_1/Loan_payments_data.csv;
Select $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13 from @s3_1/OrderDetails.csv;
Select $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13 from @s3_1/sampledata.csv;

--We can give alias Names as well for clumns
Select $1 as order_id,$2 as amount,$3 as profit,$4 as quantity,$5 as category,$6 as SCategory from @s3_1/OrderDetails.csv;

--creating the Orderdetails tables by using Infer Schema
Select * from table(Infer_schema(Location => '@s3_1', file_format => 'DBP.FF.CSV_INFER', files => ('OrderDetails.csv')));

create or replace table DBP.public.OD using template (Select ARRAY_AGG(OBJECT_CONSTRUCT(*)) from table(Infer_schema(Location => '@s3_1', file_format => 'DBP.FF.CSV_INFER', files => ('OrderDetails.csv'))));
Select * from public.od;

// Case 2: load only required fields

CREATE OR REPLACE TABLE DBP.PUBLIC.ORDERS_EX (
    ORDER_ID VARCHAR(30),
    AMOUNT INT
    );

COPY into DBP.PUBLIC.ORDERS_EX from (Select $1,$2 from @s3_1) 
File_format = (type = csv field_delimiter = ',' skip_header = 1)
files = ('OrderDetails.csv');

Select * from DBP.PUBLIC.ORDERS_EX;

// Case3: applying basic transformation by using functions

CREATE OR REPLACE TABLE DBP.PUBLIC.ORDERS_EX (
    ORDER_ID VARCHAR(30),
    PROFIT INT,
	AMOUNT INT,    
    CAT_SUBSTR VARCHAR(5),
    CAT_CONCAT VARCHAR(60),
	PFT_OR_LOSS VARCHAR(10)
  );

  copy into dbp.public.orders_ex from (
    Select  o.$1 as ORDER_ID,
            o.$3 as Profit,
            o.$2 as amount,
            substr(o.$6,1,5) as CAT_SUBSTR,
            concat(o.$5,o.$6) as CAT_CONCAT,
            case when o.$3 <=0 then 'Loss' else 'Profit' end  as PFT_OR_LOSS
            from @dbp.stgs.S3_1 o
  )
  file_format = (type = csv, field_delimiter = ',', skip_header = 1)
  files  = ('OrderDetails.csv');

  Select * from dbp.public.orders_ex;

  --Case 4: Loading sequence numbers in columns

// Create a sequence
create sequence seq1;
Select seq1.nextval;
CREATE OR REPLACE TABLE dbp.PUBLIC.LOAN_PAYMENT (
  "SEQ_ID" number default seq1.nextval,
  "Loan_ID" STRING,
  "loan_status" STRING,
  "Principal" STRING,
  "terms" STRING,
  "effective_date" STRING,
  "due_date" STRING,
  "paid_off_time" STRING,
  "past_due_days" STRING,
  "age" STRING,
  "education" STRING,
  "Gender" STRING
 );

describe table dbp.PUBLIC.LOAN_PAYMENT;
--truncate table dbp.PUBLIC.LOAN_PAYMENT;
copy into dbp.PUBLIC.LOAN_PAYMENT("Loan_ID","loan_status","Principal","terms","effective_date","due_date","paid_off_time","past_due_days","age","education","Gender")
from  (Select   l.$1 as Loan_id, 
                l.$2 as Loan_status,
                l.$3 as principal,
                l.$4 as terms,
                l.$5 as effective_date,
                l.$6 as due_date,
                l.$7 as paid_off_time,
                l.$8 as past_due_days,
                l.$9 as age,
                l.$10 as education,
                l.$11 as gender
                from @DBP.STGS.s3_1 l)
file_format = (type = csv, skip_header = 1, field_delimiter = ',')
files = ('Loan_payments_data.csv'); -- Column names should match with the table column names

COPY INTO PUBLIC.LOAN_PAYMENT("Loan_ID", "loan_status", "Principal", "terms", "effective_date", "due_date",
"paid_off_time", "past_due_days", "age", "education", "Gender")
    FROM @s3_1/Loan_payments_data.csv
    file_format = (type = csv  field_delimiter = ','  skip_header=1);  

Select * from dbp.PUBLIC.LOAN_PAYMENT;

CREATE OR REPLACE TABLE DBP.PUBLIC.LOAN_PAYMENT2 (
  "LOAN_SEQ_ID" number autoincrement start 1001 increment 1,
  "Loan_ID" STRING,
  "loan_status" STRING,
  "Principal" STRING,
  "terms" STRING,
  "effective_date" STRING,
  "due_date" STRING,
  "paid_off_time" STRING,
  "past_due_days" STRING,
  "age" STRING,
  "education" STRING,
  "Gender" STRING
 );

 COPY INTO PUBLIC.LOAN_PAYMENT2("Loan_ID", "loan_status", "Principal", "terms", "effective_date", "due_date",
"paid_off_time", "past_due_days", "age", "education", "Gender")
    FROM @s3_1/Loan_payments_data.csv
    file_format = (type = csv  field_delimiter = ','  skip_header=1); 

show schemas;
use schema stgs;
create or replace stage S3_2 
url = 's3://bucketsnowflakes4';

List @DBP.STGS.S3_2;

CREATE OR REPLACE TABLE DBP.PUBLIC.ORDERS_EX_Er (
    ORDER_ID VARCHAR(30),
    AMOUNT INT,
    PROFIT INT,
    QUANTITY INT,
    CATEGORY VARCHAR(30),
    SUBCATEGORY VARCHAR(30));

Select $1,$2,$3,$4,$5,$6 from @s3_2/OrderDetails_error.csv; --In 1st row itself Profit contains string value instead of number "One thousand"

--Demonstrating error message
copy into dbp.public.ORDERS_EX_Er from @dbp.stgs.s3_2
file_format = (format_name = dbp.ff.csv)
files = ('OrderDetails_error.csv'); -- This gives the error in Console only not in tabular format like sql syntax error

Select * from public.ORDERS_EX_Er;  --No data 

 // Error handling using the ON_ERROR option

 copy into dbp.public.ORDERS_EX_Er from @s3_2
 file_format = (format_name = dbp.ff.csv)
 files = ('OrderDetails_error.csv') 
 on_error = 'continue';  --It gives you the Error count as 2 and rows_loaded 1498 also gives you the Actual Error

Select * from dbp.public.ORDERS_EX_Er;
Select count(*) from dbp.public.ORDERS_EX_Er;

--truncate table dbp.public.ORDERS_EX_Er;

// Error handling using the ON_ERROR option = ABORT_STATEMENT (default)
 copy into dbp.public.ORDERS_EX_Er from @s3_2
 file_format = (format_name = dbp.ff.csv)
 files = ('OrderDetails_error.csv') 
 on_error = 'abort_statement'; -- it gives you Error msg on the console only not in Tabular format

 // Error handling using the ON_ERROR option = SKIP_FILE
copy into dbp.public.ORDERS_EX_Er from @s3_2
 file_format = (format_name = dbp.ff.csv)
 files = ('OrderDetails_error.csv') 
 on_error = 'SKIP_FILE'; -- It doesn't load the data in table but gives you all the Error details in Tabular format

Select * from dbp.public.ORDERS_EX_Er;
Select count(*) from dbp.public.ORDERS_EX_Er;

--truncate table dbp.public.ORDERS_EX_Er;

// Error handling using the ON_ERROR option = SKIP_FILE_<number>
copy into dbp.public.ORDERS_EX_Er from @s3_2
 file_format = (format_name = dbp.ff.csv)
 files = ('OrderDetails_error.csv','OrderDetails_error2.csv') 
 on_error = 'SKIP_FILE_2'; -- It loads the data of a file OrderDetails_error2 cause it have no errors

 Select * from dbp.public.ORDERS_EX_Er;
Select count(*) from dbp.public.ORDERS_EX_Er;

--truncate table dbp.public.ORDERS_EX_Er;

Select $1,$2,$3,$4,$5,$6 from @s3_2/OrderDetails_error2.csv; -- File has no errored records

COPY INTO DBP.PUBLIC.ORDERS_EX_Er
    FROM @s3_2
    file_format= (type = csv field_delimiter=',' skip_header=1)
    files = ('OrderDetails_error.csv','OrderDetails_error2.csv')
    ON_ERROR = SKIP_FILE_3 
    SIZE_LIMIT = 2;  -- it will gives you the info of loading data files only. won't give you the information of failed files

select current_schema();
-- Time for checking all COPY Options
create or replace stage S3_3
url = 's3://snowflakebucket-copyoption/size/';

List @S3_3;

CREATE OR REPLACE TABLE  DBP.PUBLIC.ORDERS_CPO (
    ORDER_ID VARCHAR(30),
    AMOUNT VARCHAR(30),
    PROFIT INT,
    QUANTITY INT,
    CATEGORY VARCHAR(30),
    SUBCATEGORY VARCHAR(30));

copy into DBP.PUBLIC.ORDERS_CPO from @S3_3 
file_format = (format_name = dbp.ff.csv)
pattern = '.*Order.*'
validation_mode = 'RETURN_ERRORS'; --If the files have any errors it return the Errors in Console But it doesn't load the data into  Table

copy into DBP.PUBLIC.ORDERS_CPO from @S3_3 
file_format = (format_name = dbp.ff.csv)
pattern = '.*Order.*'
validation_mode = 'RETURN_5_ROWS'; -- It returns Some data without loading it into the Table

create or replace stage S3_4 url = 's3://snowflakebucket-copyoption/returnfailed/';

list @s3_4;
copy into DBP.PUBLIC.ORDERS_CPO from @S3_4
file_format = (format_name = dbp.ff.csv)
pattern = '.*Order.*'. --'01c35070-0001-1e8d-0018-51ef001c30a2'
validation_mode = 'RETURN_ERRORS'; -- It returns all errors from the all files in stage locations in tabular format

copy into DBP.PUBLIC.ORDERS_CPO from @S3_4
file_format = (format_name = dbp.ff.csv)
pattern = '.*Order.*'
validation_mode = 'RETURN_100_ROWS';  -- It returns rows from the 4 files in the stage location if any errors are in the file return error in console only

--// Storing rejected /failed results in a table
create or replace table dbp.public.rejected as 
Select * from table(result_scan(last_query_id())); --not working

create or replace table dbp.public.rejected as 
Select rejected_record from table(result_scan('01c35070-0001-1e8d-0018-51ef001c30a2'));

Select * from dbp.public.rejected;

--drop table dbp.public.rejected;

--Working with the Errored Records
create or replace table dbp.public.rej_val as 
Select  SPLIT_PART(rejected_record,',',1) as Order_id,
        SPLIT_PART(rejected_record,',',2) as amount,
        SPLIT_PART(rejected_record,',',3) as profit,
        SPLIT_PART(rejected_record,',',4) as quantity,
        SPLIT_PART(rejected_record,',',5) as category,
        SPLIT_PART(rejected_record,',',1) as sub_category 
        from dbp.public.rejected;

Select * from dbp.public.rej_val;


copy into DBP.PUBLIC.ORDERS_CPO from @S3_4
file_format = (format_name = dbp.ff.csv)
pattern = '.*Order.*' --'01c35070-0001-1e8d-0018-51ef001c30a2'
ON_ERROR = 'CONTINUE';

--truncate table DBP.PUBLIC.ORDERS_CPO;

--Using Validate function we can able to validate the previous COPY Command loads using query ID's or _last identifier like below
Select * from table(validate(ORDERS_CPO,job_id => '01c35079-0001-1f50-0018-51ef001ce0b6'));
Select * from table(validate(ORDERS_CPO,job_id => '_last'));

--Checking the FILE SIZE_LIMIT in Copy command

List @S3_3;
Copy into DBP.PUBLIC.ORDERS_CPO from @s3_3
file_format = (format_name = dbp.ff.csv)
pattern = '.*Order.*'
SIZE_LIMIT = 20000;

Select count(*) from DBP.PUBLIC.ORDERS_CPO ;

Select * from table(validate(ORDERS_CPO,job_id => '01c3508d-0001-200c-0018-51ef001d9092'));

--Using return Failed Only option
Copy into DBP.PUBLIC.ORDERS_CPO from @s3_4
file_format = (format_name = dbp.ff.csv)
pattern = '.*Order.*'
RETURN_FAILED_ONLY = true;  --returns Error records in console but it loads the data into Table 

Select count(*) from DBP.PUBLIC.ORDERS_CPO ;

Copy into DBP.PUBLIC.ORDERS_CPO from @s3_4
file_format = (format_name = dbp.ff.csv)
pattern = '.*Order.*'
ON_ERROR = 'CONTINUE'
RETURN_FAILED_ONLY = true;  -- returns Failed records only in Tabular Format 


Select count(*) from DBP.PUBLIC.ORDERS_CPO ;

--All Good now :)