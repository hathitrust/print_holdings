data = LOAD '$INPUT' AS (ocn:long, bib:chararray, member_id:chararray, status:chararray, condition:chararray, date:chararray, enum_chron:chararray, type:chararray, issn:chararray);
    
-- break out the counts
count_data = FOREACH data GENERATE ocn, member_id,  
    (status == 'LM' ? 1 : 0) AS lm,
    (status == 'WD' ? 1 : 0) AS wd,
    (condition == 'BRT' ? 1 : 0) AS brt,
    ((status == 'LM' OR condition == 'BRT') ? 1 : 0) as access;
count_data2 = FOREACH count_data GENERATE ocn, member_id,  
    (lm is null ? 0 : lm) as lm, 
    (wd is null  ? 0 : wd) as wd, 
    (brt is null ? 0 : brt) as brt,
    (access is null ? 0 : access) as access;
  
-- group and generate
by_ocn_and_member = GROUP count_data2 BY (ocn, member_id) PARALLEL 4;
final_data = FOREACH by_ocn_and_member GENERATE flatten(group) as (ocn, member_id), COUNT(count_data2) as copy_count, SUM(count_data2.lm) as lm_count, SUM(count_data2.wd) as wd_count, SUM(count_data2.brt) as brt_count, SUM(count_data2.access) as access_count;
store final_data into '$OUTPUT';
