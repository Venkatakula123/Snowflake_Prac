Use DATABASE dbp;
use schema stgs;
List @CSV_STG;
create or replace file format dbp.ff.parquet
    type = 'parquet';
Select $1 from @CSV_STG/parquet/sales_items_data.parquet(file_format => 'dbp.ff.parquet'); --Can query to check the schema as well
SELECT 
$1:"__index_level_0__",
$1:"cat_id",
$1:"d",
$1:"date",
$1:"dept_id",
$1:"id",
$1:"item_id",
$1:"state_id",
$1:"store_id",
$1:"value" --It is like json Data only But there we need to insert data into variant data type and the we can query it. But here we can query directly
FROM @CSV_STG/parquet/sales_items_data.parquet (file_format => 'dbp.ff.parquet');
Select  $1:__index_level_0__:: int as index_level,
        $1:"cat_id:: varchar as category_id",
        $1:d:: number as d,
        DATE($1:date:: int) as date,
        $1:dept_id:: varchar as dept_id,
        $1:id:: varchar as ID ,
        $1:item_id::varchar as Item_id,
        $1:state_id:: varchar as State_id,
        $1:store_id::varchar as Store_id,
        $1:value:: number as Value from @CSV_STG/parquet/sales_items_data.parquet (file_format => 'dbp.ff.parquet');

CREATE OR REPLACE TABLE DBP.PUBLIC.PARQUET_DATA 
(
    ROW_NUMBER int,
    index_level int,
    cat_id VARCHAR(50),
    date date,
    dept_id VARCHAR(50),
    id VARCHAR(50),
    item_id VARCHAR(50),
    state_id VARCHAR(50),
    store_id VARCHAR(50),
    value int,
    Load_date timestamp default TO_TIMESTAMP_NTZ(current_timestamp)
);

copy into DBP.PUBLIC.PARQUET_DATA(ROW_NUMBER, index_level, cat_id, date, dept_id, id, item_id, state_id, store_id, value)
from (select 
            METADATA$FILE_ROW_NUMBER,
            $1:__index_level_0__::int,
            $1:cat_id::VARCHAR(50),
            DATE($1:date::int ),
            $1:"dept_id"::VARCHAR(50),
            $1:"id"::VARCHAR(50),
            $1:"item_id"::VARCHAR(50),
            $1:"state_id"::VARCHAR(50),
            $1:"store_id"::VARCHAR(50),
            $1:"value"::int
      from @CSV_STG/parquet/sales_items_data.parquet (file_format => 'dbp.ff.parquet')
);

Select * from DBP.PUBLIC.PARQUET_DATA;
--truncate table DBP.PUBLIC.PARQUET_DATA;
COPY INTO DBP.PUBLIC.PARQUET_DATA(ROW_NUMBER, index_level, cat_id, date, dept_id, id, item_id, state_id, store_id, value)
from (select 
            METADATA$FILE_ROW_NUMBER,
            $1:__index_level_0__::int,
            $1:cat_id::VARCHAR(50),
            DATE($1:date::int ),
            $1:"dept_id"::VARCHAR(50),
            $1:"id"::VARCHAR(50),
            $1:"item_id"::VARCHAR(50),
            $1:"state_id"::VARCHAR(50),
            $1:"store_id"::VARCHAR(50),
            $1:"value"::int
      from @CSV_STG)
file_format = (format_name = DBP.FF.parquet)
pattern = '.*\\.*.parquet';

Select DATE(20250421);

use schema public;
CREATE OR REPLACE TABLE employees (title VARCHAR, employee_ID INTEGER, manager_ID INTEGER);
INSERT INTO employees (title, employee_ID, manager_ID) VALUES
    ('PA',1000,100);
    ('President', 1, NULL),  -- The President has no manager.
    ('Vice President Engineering', 10, 1),
    ('Programmer', 100, 10),
    ('QA Engineer', 101, 10),
    ('Vice President HR', 20, 1),
    ('Health Insurance Analyst', 200, 20);

SELECT
     emps.title,
     emps.employee_ID,
     mgrs.employee_ID AS MANAGER_ID, 
     mgrs.title AS "MANAGER TITLE"
  FROM employees AS emps LEFT OUTER JOIN employees AS mgrs
    ON emps.manager_ID = mgrs.employee_ID
  ORDER BY mgrs.employee_ID NULLS FIRST, emps.employee_ID;

WITH RECURSIVE managers                                     -- Line 1
    (indent, employee_ID, manager_ID, employee_title)       -- Line 2
  AS                                                        -- Line 3
    (                                                       -- Line 4
                                                            -- Line 5
      SELECT '' AS indent,                                  -- Line 6
             employee_ID,                                   -- Line 7
             manager_ID,                                    -- Line 8
             title AS employee_title                        -- Line 9
        FROM employees                                      -- Line 10
        WHERE title = 'President'                           -- Line 11
                                                            -- Line 12
        UNION ALL                                           -- Line 13
                                                            -- Line 14
        SELECT indent || '--- ',                            -- Line 15
               employees.employee_ID,                       -- Line 16
               employees.manager_ID,                        -- Line 17
               employees.title                              -- Line 18
          FROM employees JOIN managers                      -- Line 19
            ON employees.manager_ID = managers.employee_ID  -- Line 20
    )                                                       -- Line 21
                                                            -- Line 22
SELECT indent || employee_title AS Title,                   -- Line 23
       employee_ID,                                         -- Line 24
       manager_ID                                           -- Line 25
  FROM managers;                                            -- Line 26


Select $1 from @CSV_STG/parquet/titanic.parquet (file_format => 'dbp.ff.parquet');