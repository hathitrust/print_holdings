-- too damn slow

SELECT
    SUM(h3.copy_count) AS cc
FROM
    holdings_htitem AS h2
    JOIN
    holdings_htitem_htmember_jn AS h3 ON (h2.volume_id = h3.volume_id)
    JOIN
    holdings_cluster_htitem_jn AS c1 ON (h2.volume_id = c1.volume_id)
    JOIN
    holdings_cluster_oclc AS c2 ON (c1.cluster_id = c2.cluster_id)
WHERE
    h3.member_id NOT IN ('chi','ind','iowa','minn','msu','nwu','osu','psu','purd','uiuc','umd','unl','uom','wisc')
    AND
    h2.item_type = 'mono'
    AND
    c2.oclc IN ('737285', '7459628', '767732260', '77186', '828184663', '83440864', '8709452', '8935748', '9225365', '18558869')



select * from holdings_cluster_oclc where oclc IN ('737285');
+------------+--------+
| cluster_id | oclc   |
+------------+--------+
|    2334202 | 737285 |
+------------+--------+
1 row in set (0.00 sec)


-- OOOOH, cluster_type = 'spm' gives us MONO!

select * from holdings_cluster_oclc hco, holdings_cluster hc where hco.cluster_id = hc.cluster_id and hco.oclc IN ('737285'); 
+------------+--------+------------+----------------+--------+---------------------+------+-------------------+--------------+--------------+
| cluster_id | oclc   | cluster_id | cost_rights_id | osici  | last_mod            | H    | total_serial_size | cluster_type | num_of_items |
+------------+--------+------------+----------------+--------+---------------------+------+-------------------+--------------+--------------+
|    2334202 | 737285 |    2334202 |              2 | 737285 | 2013-09-09 00:00:00 | NULL |              NULL | spm          |            1 |
+------------+--------+------------+----------------+--------+---------------------+------+-------------------+--------------+--------------+

-- make this guy faster and we have everything we need. needs to do full read (WHYYY??) on holdings_htitem_htmember_jn, which is a crying shame.
-- OK, now we should be faster on the h table, also sorted the memberids so the more excluding happen earlier.

EXPLAIN select count(h.volume_id) as cc
from 
    holdings_htitem_htmember_jn h 

    JOIN
    holdings_cluster_htitem_jn hchj2
    ON (hchj2.volume_id = h.volume_id)

    JOIN
    holdings_cluster hc 
    ON (hc.cluster_id = hchj2.cluster_id)
    
    JOIN
    holdings_cluster_htmember_jn hchj1 
    ON (hc.cluster_id = hchj1.cluster_id)

    JOIN
    holdings_cluster_oclc hco 
    ON (hc.cluster_id = hco.cluster_id)
where 
    hco.oclc IN ('737285')
    and 
    h.member_id NOT IN ('uom','wisc','chi','uiuc','minn','ind','osu','iowa','msu','nwu','unl','psu','umd','purd')
; 


-- OK, so I must have not known about this table??

SELECT oclc, copy_count, access_count FROM holdings_memberitem_counts WHERE oclc IN ('737285') AND member_id NOT IN ('uom','wisc','chi','uiuc','minn','ind','osu','iowa','msu','nwu','unl','psu','umd','purd') AND access_count > 0;
