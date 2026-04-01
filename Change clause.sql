use database mydb;

truncate table public.raw_applicant_staging;

Select * from public.raw_applicant_staging;

insert into public.raw_applicant_staging values (111111, 'James', 'Schwartz', 'M', 'American', '342-76-9087', '5676 Washington Street', 'High School', 5,10) ;
insert into public.raw_applicant_staging values (222222, 'Jessica', 'Escobar', 'F', 'Hispanic', '456-93-5629', '3234 WateringCan Drive', 'Undergrad', 4,10) ;
insert into public.raw_applicant_staging values (333333, 'Ben', 'Hardy', 'M', 'American', '876-98-3245', '6578 Historic Circle', 'Masters', 6,30) ;

create or replace schema vw;
create or replace view vw.vw_raw_applicant_staging as 
Select * from public.raw_applicant_staging where sex = 'M';

Select * from vw.vw_raw_applicant_staging;

--assign change_tracking for the table and a view
alter table public.raw_applicant_staging set change_tracking = true;
alter view vw.vw_raw_applicant_staging set change_tracking = true;

show tables;

Select current_timestamp(); --2026-03-18 03:05:51.573 -0700

insert into public.raw_applicant_staging values (444444, 'Anjali', 'Singh', 'F', 'Indian American', '435-87-6532', '8976 Autumn Day Drive', 'Masters', 8,20) ;
insert into public.raw_applicant_staging values (555555, 'Dean', 'Tracy', 'M', 'African', '767-34-7656', '2343 India Street', 'Undergrad', 2,50) ;

-- 

select * from public.raw_applicant_staging changes (information => default)
at(timestamp => '2026-03-18 03:05:51.573 -0700'::timestamp_tz); -- 2 records

select * from vw.vw_raw_applicant_staging changes (information => default)
at(timestamp => '2026-03-18 03:05:51.573 -0700'::timestamp_tz); -- 1 records

-- Delete Operation 
Select current_timestamp(); --2026-03-18 03:11:35.313 -0700
select * from public.raw_applicant_staging; -- 5 Records
delete from  public.raw_applicant_staging where id in(111111,222222);

select * from public.raw_applicant_staging changes (information => default)
at(timestamp => '2026-03-18 03:11:35.313 -0700'::timestamp_tz); -- 2 records ID 111111, 222222

select * from vw.vw_raw_applicant_staging changes (information => default)
at(timestamp => '2026-03-18 03:11:35.313 -0700'::timestamp_tz); -- 1 records ID 111111

--Now after delete operation let's check with information => append_only option like stream object
select * from public.raw_applicant_staging changes (information => append_only)
at(timestamp => '2026-03-18 03:11:35.313 -0700'::timestamp_tz);  --It gives nothing

select * from vw.vw_raw_applicant_staging changes (information => append_only)
at(timestamp => '2026-03-18 03:11:35.313 -0700'::timestamp_tz);  -- It gives nothing

--Lets Do the Update Operation
Select current_timestamp(); --2026-03-18 03:44:06.491 -0700
update public.raw_applicant_staging set education_level = 'COLLEGE' where ID IN(333333,444444);

select * from public.raw_applicant_staging changes (information => default)
at(timestamp => '22026-03-18 03:44:06.491 -0700'::timestamp_tz); 

select * from vw.vw_raw_applicant_staging changes (information => default)
at(timestamp => '2026-03-18 03:44:06.491 -0700'::timestamp_tz); 

Select * from public.raw_applicant_staging;
Select * from vw.vw_raw_applicant_staging;

Select CURRENT_TIMESTAMP(); --2026-03-18 03:49:54.589 -0700
update public.raw_applicant_staging set education_level = 'TUTION' where ID = 555555;

select * from public.raw_applicant_staging changes (information => default)
at(timestamp => '2026-03-18 03:49:54.589 -0700'::timestamp_tz); 

select * from vw.vw_raw_applicant_staging changes (information => default)
at(timestamp => '2026-03-18 03:49:54.589 -0700'::timestamp_tz); 

select * from public.raw_applicant_staging changes (information => append_only)
at(timestamp => '2026-03-18 03:49:54.589 -0700'::timestamp_tz);  -- 0 records because it is a upsert Operation

select * from vw.vw_raw_applicant_staging changes (information => append_only)
at(timestamp => '2026-03-18 03:49:54.589 -0700'::timestamp_tz); --0 records because it is a upsert Operation

--If we consume the data using chnage clause that data will be still available. 

create temporary table a as Select * from public.raw_applicant_staging where 1 = 2;
select * from a;

describe table a;

insert into a 
Select 
ID,
FIRST_NAME,
LAST_NAME,
SEX,
ETHINICITY,
SSN,
STREET_ADDRESS,
EDUCATION_LEVEL,
YEARS_OF_EXPERIENCE,
JOB_ID
from public.raw_applicant_staging changes (information => default)
at(timestamp => '2026-03-18 03:49:54.589 -0700'::timestamp_tz) where METADATA$ACTION = 'INSERT';

select * from public.raw_applicant_staging changes (information => default)
at(timestamp => '2026-03-18 03:49:54.589 -0700'::timestamp_tz); -- Still the data is available in change clause after consuming it 

create temporary table b as 
Select 
ID,
FIRST_NAME,
LAST_NAME,
SEX,
ETHINICITY,
SSN,
STREET_ADDRESS,
EDUCATION_LEVEL,
YEARS_OF_EXPERIENCE,
JOB_ID
from public.raw_applicant_staging changes (information => default)
at(timestamp => '2026-03-18 03:49:54.589 -0700'::timestamp_tz) where METADATA$ACTION = 'INSERT';

select * from b;


use schema mydb.streamsdb;
create table am (id number, name varchar);
insert into am values(1,'Avr'),(2,'Bvr'),(3,'Cvr'),(4,'Dvr');

create or replace stream ams on table am;

describe stream ams;