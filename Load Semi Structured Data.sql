use database dbp;
use schema public;
use schema stgs;
show stages;
show integrations;

List @S3_1;
List @CSV_STG;

create or replace file format FF.JSON_INFER
type = json;

describe file format FF.JSON_INFER;

Select * from table(INFER_SCHEMA(location => '@DBP.STGS.CSV_STG/jsonf' , FILE_FORMAT => 'DBP.FF.JSON_INFER', files => 'pets_data.json'));

Select $1 from @CSV_STG/jsonf/pets_data.json;

CREATE OR REPLACE TABLE DBP.public.PETS_DATA_JSON_RAW 
(raw_file variant);

copy into DBP.public.PETS_DATA_JSON_RAW  from @DBP.STGS.CSV_STG
file_format = (format_name = DBP.FF.JSON_INFER)
pattern = '.*\\.*pets.*.json';

Select * from DBP.public.PETS_DATA_JSON_RAW; 
--truncate table DBP.public.PETS_DATA_JSON_RAW;

Select  raw_file:Name::string as Name,
        raw_file:Gender::string as Gender,
        raw_file:DOB:: date as DOFB,
        raw_file:Address.City:: string as city,
        raw_file:Address."House Number" :: string as HNO,
        raw_file:Address.State:: string as state,
        raw_file:Phone.Mobile::string as Mno,
        raw_file:Phone.Work::string as Telno,
        raw_file:Pets[0]:: string as pet1,
        raw_file:Pets[1]:: string as Pet2  --Not best Practise to hard code once in future one more pet extra added we need to make changes again in the code
from DBP.public.PETS_DATA_JSON_RAW;

Select  raw_file:Name::string as Name,
        raw_file:Gender::string as Gender,
        raw_file:DOB:: date as DOFB,
        raw_file:Address.City:: string as city,
        raw_file:Address."House Number" :: string as HNO,
        raw_file:Address.State:: string as state,
        raw_file:Phone.Mobile::string as Mno,
        raw_file:Phone.Work::string as Telno,
        raw_file:Pets[0]:: string as pets 
from DBP.public.PETS_DATA_JSON_RAW
UNION ALL
Select  raw_file:Name::string as Name,
        raw_file:Gender::string as Gender,
        raw_file:DOB:: date as DOFB,
        raw_file:Address.City:: string as city,
        raw_file:Address."House Number" :: string as HNO,
        raw_file:Address.State:: string as state,
        raw_file:Phone.Mobile::string as Mno,
        raw_file:Phone.Work::string as Telno,
        raw_file:Pets[1]:: string as pets 
from DBP.public.PETS_DATA_JSON_RAW
union all
Select  raw_file:Name::string as Name,
        raw_file:Gender::string as Gender,
        raw_file:DOB:: date as DOFB,
        raw_file:Address.City:: string as city,
        raw_file:Address."House Number" :: string as HNO,
        raw_file:Address.State:: string as state,
        raw_file:Phone.Mobile::string as Mno,
        raw_file:Phone.Work::string as Telno,
        raw_file:Pets[2]:: string as pets 
from DBP.public.PETS_DATA_JSON_RAW;  --This is also not an essicieant approach if extra pet added on a person again we need to do make change in code 

--So for this will have method called flattn()
create table  dbp.public.Pets_data as
Select  raw_file:Name::string as Name,
        raw_file:Gender::string as Gender,
        raw_file:DOB:: date as DOFB,
        raw_file:Address.City:: string as city,
        raw_file:Address."House Number" :: string as HNO,
        raw_file:Address.State:: string as state,
        raw_file:Phone.Mobile::string as Mno,
        raw_file:Phone.Work::string as Telno,
        f1.value::string as Pets
from DBP.public.PETS_DATA_JSON_RAW,
table(flatten(raw_file:Pets)) f1;

Select * from dbp.public.Pets_data;

select Current_schema();
List @CSV_STG;
Select * from table(infer_schema(location => '@DBP.STGS.CSV_STG/jsonf', file_format=> 'DBP.FF.JSON_INFER',FILES => '4Wheelers.json'));

Select $1 from @DBP.STGS.CSV_STG/jsonf/4Wheelers.json;

create table dbp.public.Vehicle(RAW_FILE variant);

copy into dbp.public.Vehicle from @DBP.STGS.CSV_STG
file_format = (format_name = DBP.FF.JSON_INFER)
pattern = '.*\\.*4Wheelers.*.json'; --'.*\\.*pets.*.json';

Select * from dbp.public.Vehicle;

Create or replace table dbp.public.owner (
    car_model varchar, 
    car_model_year int,
    car_make varchar, 
    first_name varchar,
    last_name varchar);	

INSERT into dbp.public.owner 
Select  raw_file:"Car Model":: string as CAR_MODEL,
        raw_file:"Car Model Year":: string as CAR_MODEL_YEAR,
        raw_file:"car make":: string as CAR_MAKE,
        raw_file:"first_name"::string as first_name,
        raw_file:"last_name":: string as last_name
        from dbp.public.Vehicle;

Select * from dbp.public.owner ;

--Processing Xml
select CURRENT_SCHEMA();
List @CSV_STG;

create file format dbp.ff.xml_infer type = xml;
describe file format dbp.ff.xml_infer;
--Select * from table(INFER_SCHEMA(location => '@DBP.STGS.CSV_STG/xmlf', FILE_FORMAT=> 'DBP.FF.XML_INFER', files => '.xml' ));

Select $1 from @CSV_STG/xmlf/books_20230304.xml;
CREATE OR REPLACE TABLE dbp.public.STG_BOOKS(xml_data variant);


