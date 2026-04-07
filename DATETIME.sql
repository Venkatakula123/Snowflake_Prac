use database DBP;
use schema public;
--Snowflake DATE TIME DEFAULT FORMAT 'YYYY-MM-DD HH24:MI:SS.FF3'
--DATE -- 'YYYY-MM-DD'
--TIME -- 'HH24:MI:SS'

create or replace transient table bookings(
booking_id number,
booking_dt date,
booking_time time,
booking_dt_time datetime,
booking_timestamp timestamp);

create or replace transient table customer_f3 (
customer_pk number (38,0),
salutation varchar (10),
first_name varchar(20),
last_name varchar (30),
gender varchar(1),
marital_status varchar (1),
day_of_birth date,
birth_country varchar(60),
email_address varchar(50),
city_name varchar(60),
zip_code varchar(10),
country_name varchar (20),
gmt_timezone_offset number(10,2),
preferred_cust_flag boolean,
registration_time timestamp);

Select CURRENT_DATABASE();