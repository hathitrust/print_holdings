SELECT
    c2.oclc,
    h3.member_id,
    SUM(h3.copy_count) AS cc,
    SUM(h3.access_count) AS ac
FROM
    holdings_htitem AS h2 
    JOIN 
    holdings_htitem_htmember_jn AS h3 ON (h2.volume_id = h3.volume_id)
    JOIN
    holdings_cluster_htitem_jn AS c1 ON (h2.volume_id = c1.volume_id)
    JOIN
    holdings_cluster_oclc AS c2 ON (c1.cluster_id = c2.cluster_id)
WHERE
    h3.member_id IN ('chi','ind','iowa','minn','msu','nwu','osu','psu','purd','uiuc','umd','unl','uom','wisc')
    AND
    h2.item_type = 'mono'
GROUP BY
    c2.oclc,
    h3.member_id;