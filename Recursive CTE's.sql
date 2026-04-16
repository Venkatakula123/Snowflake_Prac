use database DBP;
use schema public;
Select CURRENT_DATABASE();

Select 1 as n
UNION all
Select 2 as n
union all
Select 3 as n
union all
Select 4 as n
union all
Select 5 as n;

Select value::number as n from table(flatten(input => split('1,2,3,4,5',',')));

with recursive my_rec as(
    Select 1 as n
    union all
    Select n+1 as n from my_rec where n < 11
)
Select n from my_rec;