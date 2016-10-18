CREATE VIEW v_shared_print_pool AS
SELECT hm.member_id, hm.status, hm.item_condition, COALESCE(o.oclc_x, hm.oclc) AS oclc
FROM holdings_memberitem AS hm LEFT JOIN oclc_resolution AS o ON (hm.oclc = o.oclc_y)
WHERE hm.item_type = 'mono';

CREATE TABLE shared_print_pool (
    id INT NOT NULL AUTO_INCREMENT,
    holdings_memberitem_id INT NOT NULL,    
    member_id VARCHAR(20) NOT NULL,
    oclc bigint(20) NOT NULL,
    PRIMARY KEY (id),
    INDEX shared_print_pool_oclc (oclc),
    INDEX shared_print_pool_member_id (member_id)    
);
