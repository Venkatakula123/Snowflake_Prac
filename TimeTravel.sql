Show parameters like '%retention%' in account;

alter  account set DATA_RETENTION_TIME_IN_DAYS = 20;

alter account set MIN_DATA_RETENTION_TIME_IN_DAYS = 5;

show parameters like '%retention%' in database;

alter database set DATA_RETENTION_TIME_IN_DAYS = 15;

show parameters like '%retention%' in schema;

Show tables in schema public; -- retension time is 15 days set at the DB Level

show parameters like '%Time%'; --America/Los_Angeles

Select CURRENT_TIMESTAMP(); --2026-03-09 07:18:46.904 -0700  

CREATE TABLE Emp (
    emp_id INT PRIMARY KEY,
    emp_name VARCHAR(100) NOT NULL,
    job_title VARCHAR(50),
    manager_id INT,
    hire_date DATE,
    salary DECIMAL(10,2),
    dept_id INT
);

INSERT INTO Emp (emp_id, emp_name, job_title, manager_id, hire_date, salary, dept_id)
VALUES
(21, 'Deepak Mishra', 'Business Analyst', 20, '2020-12-01', 47000.00, 40), (22, 'Renu Chawla', 'Developer', 6, '2023-04-05', 52000.00, 10), (23, 'Ashok Jain', 'Tester', 6, '2022-07-19', 44000.00, 10), (24, 'Farah Khan', 'HR Specialist', 14, '2021-08-22', 49000.00, 20), (25, 'Gaurav Bhatia', 'Finance Manager', NULL, '2018-05-15', 78000.00, 30), (26, 'Harini Reddy', 'Recruiter', 14, '2022-10-10', 41000.00, 20), (27, 'Imran Ali', 'System Engineer', 13, '2020-03-03', 59000.00, 40), (28, 'Jyoti Sharma', 'Project Coordinator', 20, '2021-11-11', 55000.00, 40), (29, 'Kabir Das', 'Data Analyst', 19, '2022-06-06', 60000.00, 40), (30, 'Leela Nair', 'Finance Assistant', 25, '2023-01-25', 45000.00, 30);

Select * from emp;

Select CURRENT_TIMESTAMP();

delete from emp where MANAGER_ID = 1; --01c2e9fd-0000-eb0b-0018-51ef00013746

delete from emp where salary < 50000; --01c2e9fe-0000-eba2-0018-51ef00015102

--SELECT * FROM emp before(timestamp => '2026-03-09 7:51:01 -0700'::TIMESTAMP_ltz);
SELECT * FROM emp before(statement => '01c2e9fd-0000-eb0b-0018-51ef00013746');

Select * from emp at(statement => '01c2e9fe-0000-eba2-0018-51ef00015102');

--Create temp table t_emp clone emp before(statement => '01c2e9fd-0000-eb0b-0018-51ef00013746');

Select * from t_emp; 

--truncate table t_emp;

--truncate table emp;

--insert into emp Select * from t_emp;

Select CURRENT_TIMESTAMP(); --2026-03-09 08:15:43.200 -0700
delete from emp where MANAGER_ID = 1;  --01c2ea34-0000-eb0a-0000-001851ef7465

delete from emp where salary < 50000; --01c2ea34-0000-e600-0000-001851efa421

Select TO_TIMESTAMP_ltz('3/9/2026, 8:46:57 PM', 'MM/DD/YYYY, HH12:MI:SS AM');

Select * from emp at(timestamp => '2026-03-09 20:46:57.000 -0700'::timestamp_ltz);
 
create or replace schema TT;

Alter schema set DATA_RETENTION_TIME_IN_DAYS = 0;

show parameters like '%retention%' in schema TT;

create or replace table a(num NUMBER(4),Name varchar(30)) DATA_RETENTION_TIME_IN_DAYS = 5;

show parameters like '%retention%' in table a;

insert into a values(0001,'Avr'),(0002,'Bvr');

drop table a;

undrop table a;

Show parameters like '%retention%' in table MYDB.TT.a;

--Alter a set DATA_RETENTION_TIME_IN_DAYS = 2;

SELECT
TABLE_NAME,
ACTINS_BYTES/(1024*1024*1024) AS STORAGE_USED_IN_GB,
TIME_TRAVEL_BYTES/(1024*1024*1024) AS TIME_TRAVEL_STORAGE_IN_GB,
FAILSAFE_BYTES/(1024*1024*1024) AS FAILSAFE_STORAGE_IN_GB
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS ORDER BY FAILSAFE_BYTES DESC;

Select * from SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS limit 100;
  