use role sysadmin;


-- step-1
-- create databse and schema
create or replace database demo;
create or replace schema my_sch;
use database demo;
use schema my_sch;
create or replace table customer (
 customer_id varchar(),
 first_name varchar(),
 last_name varchar(),
 gender varchar(6),
 govt_id varchar(),
 date_of_birth date,
 annual_income number(38,2),
 credit_card_number varchar(20),
 card_provider varchar(20),
 mobile_number number(20),
 address varchar(),
 created_on timestamp_ntz(9)
);


desc table customer;
INSERT INTO Customer  VALUES
('d1f6f86c-029a-4245-bb91-433a6aa79987','Mandy','Evans','Male','378-25-4428','1992-08-22',108103.56,'4516-4829-8251-8697','Visa','9164391029','13550 Morgan Pass Smithburgh, WI 76537','2024-02-04 17:36:43'),
('6a1eccda-a70c-41b5-9978-cec0495cfa4f','Daniel','Maldonado','Female','187-95-6145','1964-12-05',147782.33,'4031-7382-4968-2022','American Express','6215881561','35852 Morris Causeway Lake Seanfurt, AS 24026','2024-01-03 06:34:07'),
('372139f3-5e25-43dd-b65b-9045c5bc647a','Erika','Juarez','Female','493-80-1009','1935-02-24',76017.27,'4596-8351-8217-2537','American Express','7286257254','2818 Dwayne Shoals Blackwellville, KS 66714','2024-03-19 20:44:27'),
('f46d4e1d-c05f-49af-8a5b-257910ed0260','Steven','White','Female','189-04-6110','1978-03-31',41702.97,'4622-9836-8651-1731','Mastercard','8196693009','283 Bradley Crossroad West Kimberlyton, MN 68224','2024-01-21 21:17:44'),
('27fafa5d-1165-4edb-8242-81d3a11908e0','Nathan','Erickson','Male','037-36-7276','1983-06-13',24475.13,'4479-4477-0470-3526','Mastercard','9189343318','PSC 1019, Box 3672 APO AA 46262','2024-02-10 11:55:45'),
('50b7da40-b9b3-4ee7-8cd6-8a77e6c0bc68','Meghan','Jones','Male','309-48-0663','1990-05-01',121852.32,'4212-4356-3021-8059','Discover','9814758205','17293 Hudson Knolls Lake Justin, PR 12530','2024-01-19 06:00:18'),
('811348f1-8caa-497d-b513-6f4187111aa9','Diane','Ortiz','Female','537-02-8520','1953-11-26',40485.83,'4927-6705-0658-9052','Mastercard','9127128898','5642 Cunningham Centers South Stephanieville, RI 23322','2024-01-07 11:00:13'),
('d5c29cea-4f10-4a2e-8d1c-6a5ef0a8c3b4','Jay','Davis','Male','512-61-0965','1990-11-21',50461.15,'4352-6983-7238-9830','Discover','8199761201','31181 Joseph Freeway Apt. 437 Jessicamouth, TN 53394','2024-02-18 09:02:42'),
('d64bc69f-dd71-4d12-b35a-a0baa692beb2','Robin','Rodriguez','Male','031-66-8851','1983-05-09',40351.97,'4671-6715-3746-9122','Visa','6217016304','9208 Johnson Neck Suite 340 Richardsonside, KS 30706','2024-01-18 22:03:35'),
('4053559f-343b-435b-8b3a-ae05eb872685','Samantha','Smith','Male','155-73-9537','1951-05-31',37797.93,'4329-9484-0366-9670','Mastercard','7281146961','481 Mata Squares Suite 260 Lake Rachelville, KY 87464','2024-03-02 02:41:04'),
('64722c12-bb61-4f0c-a9a8-3797e34eb93e','Thomas','Jarvis','Female','705-83-1986','1991-12-01',117820.07,'4907-2052-4262-4592','American Express','8166412170','3970 Lambert Parks New Katrina, MN 90937','2024-02-27 23:55:21');


Select * from customer;


select current_role();
use role securityadmin;
grant usage on warehouse compute_wh to role sysadmin;
grant usage on warehouse compute_wh to role public;
grant usage on warehouse compute_wh to role useradmin;
grant usage on database demo to role useradmin;
grant usage on database demo to role public;
grant usage on schema demo.my_sch to role public;
grant usage on schema demo.my_sch to role useradmin;


grant select on table demo.my_sch.customer TO ROLE public;
grant select on table demo.my_sch.customer TO ROLE useradmin;


use role sysadmin;
use schema demo.my_sch;
use warehouse compute_wh;




--Creating the Dynamic Masking Policy
select CURRENT_SCHEMA();
Select * from my_sch.customer;
create or replace masking policy PII_MASK as (pii_text string) returns string ->
    case
        when current_role() in ('SYSADMIN') then pii_text
        when current_role() in ('USERADMIN') then regexp_replace(pii_text,substring(pii_text,1,7),'xxxx-xxx-xx-x')
        else '**-MASKED-**' end;


describe masking policy PII_MASK;


show masking policies;


Select get_ddl('POLICY','PII_MASK');
Select * from SNOWFLAKE.ACCOUNT_USAGE.POLICY_REFERENCES;


--setting up the Masking Policy on Customer table gov_id column
alter table customer modify column govt_id set masking policy pii_mask;
alter table customer modify column govt_id unset masking policy;


use role useradmin;
Select * from customer;
use role accountadmin;
Select * from customer;


use role public;
Select * from customer;


Create or replace masking policy cre_card as (card_number string) returns string ->
    case    when current_role() in ('SYSADMIN') then card_number
            when current_role() in ('USERADMIN') then regexp_replace(card_number,substring(card_number,1,14),'XXXX-XXXX-XXXX-' )
            else '*-MASKING CARD-*' END;


alter table customer modify column credit_card_number set masking policy cre_card;
use role accountadmin;
use role sysadmin;
use role useradmin;
use role public;


Select * from customer;


create or replace masking policy dob_mask as (dob date) returns date ->
    case    when current_role() IN ('SYSADMIN') then dob
            else '1999-01-01' end;
   
select current_role();


alter table customer modify column date_of_birth set masking policy dob_mask;


alter masking policy pii_mask
set body ->
    case
        when current_role() in ('SYSADMIN')
                then pii_text
        when current_role() in ('USERADMIN' ) then
                case
                    when len(pii_text) = 11 and pii_text LIKE '%-%' then
                            regexp_replace(pii_text, substring(pii_text, 1,7), 'xxx-xx-')
                    else
                        '***Type-Masked***'
                    end
            else '*** Masked ***'
    end;


alter table customer modify column card_provider set masking policy pii_mask;


--we can't be drop or replace the policy if it was attaxhed to a column in a table;


create or replace masking policy ann_income as (income number) returns number ->
case    when current_role() in ('SYSADMIN') then   income
        else -990.00
        end;


alter table customer modify column annual_income set masking policy ann_income;