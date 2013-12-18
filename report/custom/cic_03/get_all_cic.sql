SELECT
    h3.volume_id,
    h3.member_id,
    SUM(h3.copy_count) AS cc,
    SUM(h3.access_count) AS ac
FROM
    holdings_htitem AS h2 
    JOIN 
    holdings_htitem_htmember_jn AS h3 ON (h2.volume_id = h3.volume_id)
WHERE
    h3.member_id IN ('chi','ind','iowa','minn','msu','nwu','osu','psu','purd','uiuc','umd','unl','uom','wisc')
    AND
    (h2.item_type = 'mono' OR h2.item_type = 'multi')
GROUP BY
    h3.volume_id,
    h3.member_id;