--DROP TABLE athlete_events
SELECT * FROM athlete_events


select Sex
,Age
,Height
,NOC
,Sport
,WINBIT
 from (
select 
Sex
,Age
,Height
,NOC
,Sport
, case when Medal='NA' then 0 else 1 end as WINBIT
,case when ROW_NUMBER() OVER(ORDER BY name ASC)%2>0 then 1 else 0 end as oddbit
from athlete_events)a
where oddbit=1


