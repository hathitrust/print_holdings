

select count(distinct ho.volume_id) as allow, 0 as deny from holdings_memberitem_iowa as m, holdings_htitem_oclc as ho, holdings_htitem as h where m.oclc = ho.oclc and h.volume_id = ho.volume_id and access = 'allow'
UNION
select 0 as allow, count(distinct ho.volume_id) as deny from holdings_memberitem_iowa as m, holdings_htitem_oclc as ho, holdings_htitem as h where m.oclc = ho.oclc and h.volume_id = ho.volume_id and access = 'deny'



-- first
+--------+------+
| allow  | deny |
+--------+------+
| 524920 |    0 |
+--------+------+

-- second
+-------+---------+
| allow | deny    |
+-------+---------+
|     0 | 1781108 |
+-------+---------+




SELECT SUM(allow), SUM(deny) FROM (
    SELECT COUNT(DISTINCT ho1.volume_id) AS allow, 0 AS deny
    FROM  holdings_memberitem_iowa  AS m1
    INNER JOIN holdings_htitem_oclc AS ho1 ON (m1.oclc = ho1.oclc)
    INNER JOIN holdings_htitem      AS h1  ON (ho1.volume_id = h1.volume_id)
    WHERE h1.access = 'allow'
UNION
    SELECT 0 AS allow, COUNT(DISTINCT ho2.volume_id) AS deny
    FROM  holdings_memberitem_iowa  AS m2
    INNER JOIN holdings_htitem_oclc AS ho2 ON (m2.oclc = ho2.oclc)
    INNER JOIN holdings_htitem      AS h2  ON (ho2.volume_id = h2.volume_id)
    WHERE h2.access = 'deny'
) AS x;