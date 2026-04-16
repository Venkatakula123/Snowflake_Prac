use database DBP;
create or replace schema sqlp;
use schema sqlp;
CREATE TABLE EMP_T(NAME VARCHAR(100) ) ;
INSERT INTO EMP_T VALUES ( 'PRAVEEN' );
INSERT INTO EMP_T VALUES( 'RAMESH' );
INSERT INTO EMP_T VALUES('PRATAP' );
INSERT INTO EMP_T VALUES('AKHIL');
INSERT INTO EMP_T VALUES('ARYA');

Select * from EMP_T;
Select name, SUBSTR(NAME,1,1), LEFT(NAME,1), SUBSTR(Name, -1,1), RIGHT(Name,1) from EMP_T;
Select name from EMP_T where SUBSTR(name,1,1) = SUBSTR(Name,-1,1); -- Using SUBSTR
Select name from EMP_T where LEFT(NAME,1) = RIGHT(Name,1);-- Using LEFT() and RIGHT()
--Using Pattern 
Select NAME from EMP_T where NAME LIKE 'A'||'%'||'A';
Select NAME from EMP_T where NAME LIKE 'P%P';
Select NAME from EMP_T where NAME LIKE SUBSTR(name,1,1)||'%'||SUBSTR(NAME,1,1);
Select NAME from EMP_T where Name like LEFT(NAME,1)||'%'||LEFT(NAME,1);

Select Name, LEAD(name) over(order by name) as n_val from emp_t; 
Select name, lag(name) over(order by name) as p_val from emp_t;

Create table gender(name varchar(100), gender varchar(10));
Insert into gender values('PRAVEEN', 'MALE');
Insert into gender values('RAMESH', 'MALE');
Insert into gender values('PRATAP', 'MALE');                    
Insert into gender values('AKHIL', 'MALE');
Insert into gender  values('ARYA', 'FEMALE');           
INSERT INTO gender VALUES('ANYA', 'FEMALE');
INSERT INTO gender VALUES('ANITA', 'FEMALE');
INSERT INTO gender VALUES('ANANYA', 'FEMALE');
INSERT INTO gender VALUES('ANJALI', null);
SELECT e.name, e.gender FROM gender e;

SELECT e.name,  CASE WHEN e.gender = 'MALE' THEN '1'
                     WHEN e.gender = 'FEMALE' THEN '0'
                     else '$' end as Gen from gender e;

Select name, CASE   WHEN gender = 'MALE' then '1' 
                    when gender = 'FEMALE' then '0'
                    when gender is null then '$' 
                    end  gen from gender;
Select  * from gender;
Select name, DECODE(gender, 'MALE','1','FEMALE','0',null,'$') from gender;

-- 532486 --> 234568 
with CTE as (
    Select 1 as ID,SUBSTR('532486',1,1) val --Anchor Query
    union all
    Select ID+1, SUBSTR('532486',ID+1,1) from CTE where ID < LENGTH('532486') --This is the Recursive Query    
)
Select  LISTAGG(val,'-') within group(order by val)  from cte;


with CTE as (
    Select 1 as ID,SUBSTR('532486',1,1) val --Anchor Query
    union all
    Select ID+1, SUBSTR('532486',ID+1,1) from CTE where ID < LENGTH('532486') --This is the Recursive Query    
)
Select  LISTAGG(val) within group(order by val)  from cte;

Create table at(ID int, name varchar(100));
INSERT INTO at VALUES(10,'A');
INSERT INTO at VALUES(10,'B');
INSERT INTO at VALUES(10,'C');
INSERT INTO at VALUES(20,'D');  
INSERT INTO at VALUES(20,'E');
Select * from AT;
Select ID,LISTAGG(NAME,'-')  from "DBP"."SQLP"."AT" group by ID order by ID;
show tables in schema sqlp;
create table orders (
 order_id int identity(1,1),
 customer_name varchar(50),
 product varchar(50),
 order_date date
);
insert into orders (customer_name,product,order_date) values 
('Ravi', 'Laptop', '2024-01-10'),
('Arun', 'Tablet', '2024-01-12'),
('Sita', 'Mobile', '2024-01-11'),
('Ravi', 'Laptop', '2024-01-10'),
('Kiran', 'Headphones', '2024-01-13'),
('Kiran', 'Headphones', '2024-01-13'),
('Meena', 'Camera', '2024-01-14'),
('Arun', 'Tablet', '2024-01-12');

Select o.*,row_number() over(partition by o.customer_name,o.product,o.order_date order by o.order_id asc) as rn from orders o order by order_id asc;

with cte as (
    Select o.*,row_number() over(partition by o.customer_name,o.product,o.order_date order by o.order_id asc) as rn from orders o order by order_id asc
)
Select order_id,rn from cte where rn > 1;

with cte as (
    Select o.*, row_number() over (partition by o.customer_name, o.product, o.order_date order by o.order_id asc) as rn
    from orders o
)
Delete from orders where order_id IN (Select order_id from cte where rn > 1);