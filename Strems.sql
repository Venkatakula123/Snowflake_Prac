use database mydb;

create schema streamsDB;

create or replace table public.raw_applicant_staging(
id number,first_name varchar,last_name varchar,sex varchar,ethinicity varchar,ssn varchar,
street_address varchar,education_level varchar,years_of_experience number,job_id number);

select * from public.raw_applicant_staging;

create or replace table public.job_details (
job_id number,name varchar,city varchar,state varchar,
education_level varchar);

insert into public.job_details values (10, 'Painter', 'Raleigh', 'NC', 'High School') ;
insert into public.job_details values (20, 'Software Engineer', 'Raleigh', 'NC', 'Masters') ;
insert into public.job_details values (30, 'Data Architect', 'Raleigh', 'NC', 'Undergrad') ;
insert into public.job_details values (40, 'Vice President', 'Raleigh', 'NC', 'Masters') ;
insert into public.job_details values (50, 'Associate', 'Raleigh', 'NC', 'Masters') ;


create or replace table public.candidates (
id number,first_name varchar,Last_name varchar,sex varchar,ethinicity varchar,ssn varchar,street_address varchar,
candidate_education_level varchar,years_of_experience number,job_id number,job_name varchar,job_city varchar,
job_state varchar,required_education_level varchar,status varchar,comments varchar);

insert into candidates
values (100000, 'James','Schwartz', 'M', 'American', '342-76-9087', '5676 Washington Street', 'High School', 5,10, 'Painter',
'Raleigh', 'NC','High School', 'SHORTLISTED', 'The education level of candidate matched with job') ;

Select * from public.candidates;

create or replace stream strm_applicant1 on table public.raw_applicant_staging;

show streams;

describe stream strm_applicant1;

insert into public.raw_applicant_staging values (111111, 'James', 'Schwartz', 'M', 'American', '342-76-9087', '5676 Washington Street', 'High School', 5,10) ;
insert into public.raw_applicant_staging values (222222, 'Jessica', 'Escobar', 'F', 'Hispanic', '456-93-5629', '3234 WateringCan Drive', 'Undergrad', 4,10) ;
insert into public.raw_applicant_staging values (333333, 'Ben', 'Hardy', 'M', 'American', '876-98-3245', '6578 Historic Circle', 'Masters', 6,30) ;
insert into public.raw_applicant_staging values (444444, 'Anjali', 'Singh', 'F', 'Indian American', '435-87-6532', '8976 Autumn Day Drive', 'Masters', 8,20) ;
insert into public.raw_applicant_staging values (555555, 'Dean', 'Tracy', 'M', 'African', '767-34-7656', '2343 India Street', 'Undergrad', 2,50) ;

insert into raw_applicant_staging values    (666666, 'Maria', 'Lopez', 'F', 'Hispanic', '654-23-9876', '7890 Sunset Boulevard', 'PhD', 9,15) ,
                                            (777777, 'David', 'Kim', 'M', 'Asian American', '321-45-6789', '4567 Maple Avenue', 'Undergrad', 3,25) ,
                                            (888888, 'Sophia', 'Martinez', 'F', 'Latina', '987-65-4321', '1234 Ocean Drive', 'High School', 7,40) ,
                                            (999999, 'Ethan', 'Brown', 'M', 'American', '213-54-6789', '6789 Pine Street', 'Masters', 5,35) ,
                                            (101010, 'Priya', 'Reddy', 'F', 'Indian', '876-12-3456', '3456 Lotus Lane', 'PhD', 10,20) ;
Select * from strm_applicant1; --default Stream. It will capture all the Inserts and Deletes as well

update public.raw_applicant_staging set job_id = 10 where ID = 555555;  

delete from public.raw_applicant_staging where ID = 555555; 
--shows 9 rows only before consuming the Offset in the stream Object. If it is consumed by the some other process it will have all the History like if an update happend on a record the METADATA@ACTION of a record is INSERT and DELET.

Select * from mydb.streamsdb.strm_applicant1;
Select * from MYDB.public.candidates;

-- Now we are going to consume the stream offset data using merge command to push the data in Candidate Table like below.
Merge into candidates tgt using (
Select  id,
        first_name,
        last_name,
        sex,
        ethinicity,
        ssn,
        street_address,
        str.education_level as ed_level,
        years_of_experience,
        jdtl.job_id,
        name,
        city,
        state,
        jdtl.education_level as jreq_level,
        case    when str.education_level = jdtl.education_level then 'shortlisted'
                else 'rejected' end as status,
        case    when str.education_level = jdtl.education_level then 'The education level of candidate matched with job'
                else 'Application received from candidate' end as comments,
        str.metadata$action,
        str.metadata$isupdate,
        from mydb.streamsdb.strm_applicant1 str inner join mydb.public.job_details jdtl on (str.job_id = jdtl.job_id)) src on tgt.id = src.id
        when not matched and src.metadata$action = 'insert' 
                         and meatadata$isupdate = 'false' then 
                        insert values(id,first_name,last_name, sex, ethinicity, ssn, street_address, ed_level, years_of_experience,job_id,
                                        name, city, state, jreq_level,status, comments);

merge into candidates tgt
using
(
    select
        id,
        first_name,
        last_name,
        sex,
        ethinicity,
        ssn,
        street_address,
        str.education_level as ed_level,
        years_of_experience,
        jdtl.job_id,
        name,
        city,
        state,
        jdtl.education_level as jreq_level,
        case 
            when str.education_level = jdtl.education_level 
            then 'SHORTLISTED' 
            else 'STAGED' 
        end as status,
        case 
            when str.education_level = jdtl.education_level 
            then 'The education level of candidate matched with job'
            else 'Application received from candidate'
        end as comments,
        str.metadata$action,
        str.metadata$isupdate
    from strm_applicant1 str 
    inner join job_details jdtl 
        on (str.job_id = jdtl.job_id)
) src
on tgt.id = src.id

-- Insert Clause
when not matched 
     and src.metadata$action = 'INSERT' 
     and metadata$isupdate = 'FALSE'
then insert
values (
    id,
    first_name,
    last_name,
    sex,
    ethinicity,
    ssn,
    street_address,
    ed_level,
    years_of_experience,
    job_id,
    name,
    city,
    state,
    jreq_level,
    status,
    comments
);

Select * from public.candidates;

create or replace stream strm_applicant2 on table mydb.public.raw_applicant_staging append_only = true;

show streams;

--truncate table public.raw_applicant_staging;
--truncate table public.candidates;
--truncate table public.job_details;

--create or replace temporary table a as Select * from strm_applicant1 where 1=2;
--create or replace temporary table a as Select * from strm_applicant2 where 1=2;

Select * from strm_applicant1;
Select * from strm_applicant2;

Select * from public.raw_applicant_staging;

delete from public.raw_applicant_staging where id in (111111,222222,333333);
--check the both streams now find the difference

update public.raw_applicant_staging set education_level = 'school' where id = 555555;
--check the both stream again find the differences
--stream2 -- won't change
--stream1 -- changes detect automatically

truncate table public.raw_applicant_staging;
--check the both stream again find the differences
--stream2 -- won't change
--stream1 -- data is gone

--Now, I don’t want to store in my stream i.e. I want to empty my stream
--Insert data into  temporary table by creating it by giving the temporary false condition like 1 = 2 in the query
create or replace temporary table a as Select * from MYDB.STREAMSDB.STRM_APPLICANT1 where 1 = 2; -- Stream data Offset consumed by the table.

--Multiple consumers with streams. creating multiple streams on a table public.raw_applicant_staging to see the behaviour of streams
create or replace stream trm_applicant_avr on table public.raw_applicant_staging;
create or replace stream trm_applicant_bvr on table public.raw_applicant_staging;

create or replace table public.raw_applicant_staging_avr as Select * from public.raw_applicant_staging where 1 = 2;
create or replace table public.raw_applicant_staging_bvr as Select * from public.raw_applicant_staging where 1 = 2;

--I am going to insert some data into the public.raw_applicant_staging table and will do small operation on it like update delete inserts 
--Check the streams 
Select * from trm_applicant_avr;
Select * from trm_applicant_bvr;

--Now i am going to consume the stream trm_applicant_avr and check both streams behaviour
insert into public.raw_applicant_staging_avr select id,first_name,last_name,sex,ethinicity,ssn,street_address,education_level,years_of_experience,job_id
from trm_applicant_avr;

--Now observe the behaviour of streams and insert 2 more rows in base table
--trm_applicant_avr have 2 rows latest records
--trm_applicant_bvr has the 7 rows data because it is not consumed technically the Offset not moved 

--If bvr consume the data in the stream trm_applicant_bvr the offset data will be moved no data will be there in the stream 
