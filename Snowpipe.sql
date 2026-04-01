use database dbp;
use schema public;
create schema pipes;
create schema SPIPE;
use schema SPIPE;
CREATE OR REPLACE TABLE dbp.spipe.emp_data 
(
  id INT,
  first_name STRING,
  last_name STRING,
  email STRING,
  location STRING,
  department STRING
);
describe file format DBP.FF.CSV;
use schema pipes;
copy into dbp.spipe.emp_data  from @DBP.STGS.CSV_STG
file_format = (format_name = DBP.FF.CSV)
pattern = '.*\\.*employee.*.csv';
Select * from DBP.public.emp;
Select * from DBP.public.employees;
Select * from dbp.spipe.emp_data;
--truncate table dbp.spipe.emp_data;
select current_schema();

Create pipe csv_pipe
auto_ingest = true
as
copy into dbp.spipe.emp_data  from @DBP.STGS.CSV_STG
file_format = (format_name = DBP.FF.CSV)
pattern = '.*\\.*employee.*.csv';

describe pipe csv_pipe;

SELECT SYSTEM$PIPE_STATUS('csv_pipe');
/*{"executionState":"RUNNING",
    "pendingFileCount":0,
    "lastIngestedTimestamp":"2026-04-01T02:17:21.636Z",
    "lastIngestedFilePath":"path2/sp_employee_3.csv",
    "notificationChannelName":"arn:aws:sqs:eu-north-1:246314648852:sf-snowpipe-AIDATSWL7IUKD7252L4DU-PUe1-RGP6WLi-IYCsdphvg",
    "numOutstandingMessagesOnChannel":1,
    "lastReceivedMessageTimestamp":"2026-04-01T02:17:21.017Z",
    "lastForwardedMessageTimestamp":"2026-04-01T02:17:22.338Z",
    "lastPulledFromChannelTimestamp":"2026-04-01T02:24:35.941Z",
    "lastForwardedFilePath":"mlpath/path1/path2/sp_employee_3.csv"}*/

SELECT * FROM TABLE( INFORMATION_SCHEMA.COPY_HISTORY
	(TABLE_NAME  =>  'dbp.spipe.emp_data',
	 START_TIME => DATEADD(HOUR, -10 ,CURRENT_TIMESTAMP()))
);

// Step3: Validate the pipe load
SELECT * FROM TABLE(INFORMATION_SCHEMA.VALIDATE_PIPE_LOAD
	(PIPE_NAME => 'dbp.pipes.csv_pipe',
     START_TIME => DATEADD(HOUR,-2,CURRENT_TIMESTAMP()))
);

Select * from DBP.INFORMATION_SCHEMA.LOAD_HISTORY limit 100; -- Here we can seel all the COPY History of all tables
--including Failures as well
Select * from DBP.INFORMATION_SCHEMA.PIPES;
Select * from DBP.INFORMATION_SCHEMA.SEQUENCES;
use schema stgs;
Select * from DBP.INFORMATION_SCHEMA.VIEWS;

Select * from SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY;
Select * from SNOWFLAKE.ACCOUNT_USAGE.COPY_FILES_HISTORY;
Select * from SNOWFLAKE.ACCOUNT_USAGE.LOAD_HISTORY;
Select * from SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY;
Select * from SNOWFLAKE.ACCOUNT_USAGE.QUERY_ACCELERATION_ELIGIBLE;
Select * from SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY;
Select * from SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_EVENTS_HISTORY;
Select * from SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY;

select *
  from TABLE(INFORMATION_SCHEMA.VALIDATE_PIPE_LOAD
	(PIPE_NAME => 'dbp.pipes.csv_pipe',
     START_TIME => DATEADD(HOUR,-2,CURRENT_TIMESTAMP())));