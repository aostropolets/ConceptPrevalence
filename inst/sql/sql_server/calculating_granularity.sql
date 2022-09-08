/***************************************************
Calculations for data source granularity estimations
***************************************************/


set extra_float_digits = 3

-- import files: count_source for non-standard concepts and count_standard for standard ones
create table count_source
(
  concept_id int,
  cnt        int,
  type       varchar,
  database   varchar
);

create table count_standard
(
  concept_id int,
  cnt        int,
  type       varchar,
  database   varchar
);


/***************
    APPROACH 1
***************/
--option 1: two levels up from ICD10 5 digit, then all the way down
drop table if exists temp_icd;
create table temp_icd
as
with a as (
  select distinct ancestor_concept_id,
                  min_levels_of_separation,
                  max(min_levels_of_separation) over (partition by descendant_concept_id) as max
  from concept c
         join concept_relationship cr on c.concept_id = concept_id_1
         join concept_ancestor ca on ca.descendant_concept_id = concept_id_2
         join concept cc
              on cc.concept_id = ancestor_concept_id and cc.vocabulary_id = 'SNOMED' and cc.domain_id = 'Condition'
       --join count_standard cs on cs.concept_id = cc.concept_id
  where c.vocabulary_id = 'ICD10CM'
    and c.concept_code!~'^R|^S|^V|^Z'
    and length(c.concept_code) = 5
    and relationship_id = 'Maps to'
    and cr.invalid_reason is null
    and min_levels_of_separation
      <2
    and cc.concept_id!=441840
),
     b as (select ancestor_concept_id from a where min_levels_of_separation = max)
select *
from concept_ancestor
       join b using (ancestor_concept_id);


-- 2 levels up and down only from this concept
drop table if exists temp_icd_0;
create table temp_icd_0
as
-- down
select distinct ancestor_concept_id, descendant_concept_id, min_levels_of_separation
from concept c
       join concept_relationship cr on c.concept_id = concept_id_1
       join concept_ancestor ca on ca.ancestor_concept_id = concept_id_2
       join concept cc on cc.concept_id = concept_id_2
where c.vocabulary_id = 'ICD10CM'
  and length(c.concept_code) = 5
  and c.concept_code!~'^R|^S|^V|^Z'
  and cc.vocabulary_id = 'SNOMED'
  and cc.domain_id = 'Condition'
  and relationship_id = 'Maps to'
  and cr.invalid_reason is null;

-- up 1
insert into temp_icd_0
select distinct cr2.concept_id_1, cr2.concept_id_2, -1
from concept c
       join concept_relationship cr on c.concept_id = concept_id_1
       join concept cc on cc.concept_id = cr.concept_id_2
       join concept_relationship cr2 on cr2.concept_id_1 = cr.concept_id_2
where c.vocabulary_id = 'ICD10CM'
  and c.concept_code!~'^R|^S|^V|^Z'
  and length(c.concept_code) = 5
  and cc.vocabulary_id='SNOMED'
  and cc.domain_id = 'Condition'
  and cr.relationship_id = 'Maps to'
  and cr.invalid_reason is null
  and cr2.relationship_id = 'Is a'
  and cr2.invalid_reason is null;
-- up 2
insert into temp_icd_0
select distinct cr2.concept_id_1, cr3.concept_id_2, -2
from concept c
       join concept_relationship cr on c.concept_id = concept_id_1
       join concept cc on cc.concept_id = cr.concept_id_2
       join concept_relationship cr2 on cr2.concept_id_1 = cr.concept_id_2
       join concept_relationship cr3 on cr3.concept_id_1 = cr2.concept_id_2
where c.vocabulary_id = 'ICD10CM'
  and c.concept_code!~'^R|^S|^V|^Z'
  and length(c.concept_code) = 5
  and cc.vocabulary_id='SNOMED'
  and cc.domain_id = 'Condition'
  and cr.relationship_id = 'Maps to'
  and cr.invalid_reason is null
  and cr2.relationship_id = 'Is a'
  and cr2.invalid_reason is null
  and cr3.relationship_id = 'Is a'
  and cr3.invalid_reason is null;

--option 2: 5 levels down from icd10 3 digit
drop table if exists temp_icd;
create table temp_icd
as
select distinct ancestor_concept_id, descendant_concept_id, min_levels_of_separation
from concept c
       join concept_relationship cr on c.concept_id = concept_id_1
       join concept_ancestor ca on ca.ancestor_concept_id = concept_id_2
       join concept cc on cc.concept_id = descendant_concept_id
  and cc.vocabulary_id = 'SNOMED' and cc.domain_id = 'Condition'
where c.vocabulary_id = 'ICD10CM'
  and c.concept_code!~'^R|^S|^V|^Z'
  and length(c.concept_code) = 3
  and relationship_id = 'Maps to'
  and cr.invalid_reason is null
--and min_levels_of_separation<8
;


-- both approaches merge
drop table if exists temp_icd_2;
create table temp_icd_2
as
-- get a sum of counts for each hierachical tree
with a as (
  select distinct sum(cnt) over (partition by ancestor_concept_id, database)                           as sum_tree,
                  sum(cnt) over (partition by ancestor_concept_id, database, min_levels_of_separation) as sum_level,
    database,
    ancestor_concept_id,
    min_levels_of_separation
  from temp_icd
    join count_standard
  on concept_id = descendant_concept_id and type ='condition'
)
     -- calculate the % for each level within each tree
select distinct ((sum_level::float) / (sum_tree)) * 100 as percent_level, -- % at a level within each tree
  database, min_levels_of_separation, ancestor_concept_id
from a;


drop table if exists temp_icd_3;
create table temp_icd_3
as
with a as (select count(distinct ancestor_concept_id) as cnt, database
           from temp_icd_2
           group by database),
     b as (
       select distinct sum(percent_level) over (partition by database, min_levels_of_separation) as sum,
                       cnt,
         database, min_levels_of_separation
       from temp_icd_2
         join a using (database)
     )
select min_levels_of_separation, database, sum /cnt as percent
from b;

select *
from temp_icd_3;


/***************
    APPROACH 2
***************/

-- pencentiles, fixed ancestor - Clinical finding
select distinct database,
  percentile_cont(0.03) within
group (
order by min_levels_of_separation
)
as
three
,
percentile_cont
(
0.25
)
within
group (order by min_levels_of_separation) as twenty_five,
  percentile_cont(0.5) within
group (order by min_levels_of_separation) as median,
  percentile_cont(0.75) within
group (order by min_levels_of_separation) as seventy_five,
  percentile_cont(0.97) within
group (order by min_levels_of_separation) as ninety_seven

from count_standard join concept_ancestor
on descendant_concept_id = concept_id
where ancestor_concept_id = 441840 and type = 'condition'
group by database;


--weight by # of concepts
with a as (
  select distinct sum(cnt) over (partition by database) as sum_gen, database
  from count_standard join concept using (concept_id)
  where type = 'condition' and domain_id = 'Condition'),
     b as (select ((cnt::float) / (sum_gen)) * 100 as cnt, concept_id, database
           from count_standard join a using (database)
           where type = 'condition'),
     c as (select min_levels_of_separation * cnt as cnt, database
           from b
             join concept_ancestor
           on descendant_concept_id = concept_id
           where ancestor_concept_id = 441840)
select distinct database,
  percentile_cont(0.03) within
group (order by cnt) as three,
  percentile_cont(0.25) within
group (order by cnt) as twenty_five,
  percentile_cont(0.5) within
group (order by cnt) as median,
  percentile_cont(0.75) within
group (order by cnt) as seventy_five,
  percentile_cont(0.97) within
group (order by cnt) as ninety_seven
from c
group by database;


-- create the same table with ancestors
with a as (
  select distinct sum(cnt) over (partition by database) as sum_gen, database
  from count_standard
    join concept using (concept_id)
  where type = 'condition'
    and domain_id = 'Condition'),
     b as (select ((cnt::float) * 100 / (sum_gen)) as cnt, concept_id, database
           from count_standard
             join a using (database)
           where type = 'condition'),
     c as (
       select cnt, concept_id, min_levels_of_separation, database
       from b
         join concept_ancestor
       on descendant_concept_id = concept_id
       where ancestor_concept_id = 441840)
select distinct sum(cnt) over (partition by database, min_levels_of_separation), min_levels_of_separation, database
from c;


-- log
with a as (
  select distinct sum(cnt) over (partition by database) as sum_gen, database
  from count_standard
    join concept using (concept_id)
  where type = 'condition'
    and domain_id = 'Condition'),
     b as (select (log(cnt::float * 100 / (sum_gen))) as cnt, concept_id, database
           from count_standard
             join a using (database)
           where type = 'condition'),
     c as (
       select cnt, concept_id, min_levels_of_separation, database
       from b
         join concept_ancestor
       on descendant_concept_id = concept_id
       where ancestor_concept_id = 441840)
select distinct sum(cnt) over (partition by database, min_levels_of_separation), min_levels_of_separation, database
from c;


/***************
    APPROACH 3
***************/

-- 20 cherry-picked concepts
drop table if exists temp_icd_p;
create table temp_icd_p
as
select distinct ancestor_concept_id, descendant_concept_id, min_levels_of_separation
from concept_ancestor
where ancestor_concept_id in
      (4201745, 4244662, 4101673, 4155081, 4171379, 4176644, 4179872, 4190076, 31821, 134057, 320136, 376337, 432910,
       440363, 4008701);

-- 24 cherry-picked concepts
drop table if exists temp_icd_m;
create table temp_icd_m
as
select distinct ancestor_concept_id, descendant_concept_id, min_levels_of_separation
from concept_ancestor
where ancestor_concept_id in
      (select concept_id
       from concept
       where concept_code in
             ('55342001', '414029004', '362971004', '111590001', '362970003', '299691001', '362969004',
              '74732009', '118940003', '128127008', '362966006', '271983002', '49601007', '50043002', '53619000',
              '80659006', '928000', '42030000',
              '362972006', '173300003', '362973001', '414025005', '66091009')
         and vocabulary_id = 'SNOMED');


drop table if exists temp_icd_2;
create table temp_icd_2
as
-- get a sum of counts for each hierachical tree
with a as (
  select distinct sum(cnt) over (partition by ancestor_concept_id, database)                           as sum_tree,
                  sum(cnt) over (partition by ancestor_concept_id, database, min_levels_of_separation) as sum_level,
    database,
    ancestor_concept_id,
    min_levels_of_separation
  from temp_icd_m2
    join count_standard
  on concept_id = descendant_concept_id and type ='condition'
)
     -- calculate the % for each level within each tree
select distinct ((sum_level::float) / (sum_tree)) * 100 as percent_level, -- % at a level within each tree
  database, min_levels_of_separation, ancestor_concept_id
from a;


drop table if exists temp_icd_3;
create table temp_icd_3
as
with a as (select count(ancestor_concept_id) as cnt, database
           from temp_icd_2
           group by database),
     b as (
       select distinct sum(percent_level) over (partition by database, min_levels_of_separation) as sum,
                       cnt,
         database, min_levels_of_separation
       from temp_icd_2
         join a using (database)
     )
select min_levels_of_separation, database, sum /cnt as percent
from b;
