-- oclc is the resolved oclc number, it has been looked up in oclc_resolution
-- local_oclc is the oclc number the member uses
CREATE TABLE shared_print_pool (
    id INT NOT NULL AUTO_INCREMENT,
    holdings_memberitem_id INT NOT NULL,    
    member_id VARCHAR(20) NOT NULL,
    oclc bigint(20) NOT NULL,
    local_oclc bigint(20) NOT NULL,
    local_h SMALLINT NULL DEFAULT NULL,
    PRIMARY KEY (id),
    INDEX shared_print_pool_oclc (oclc),
    INDEX shared_print_pool_local_oclc (local_oclc),
    INDEX shared_print_pool_member_id (member_id),
    INDEX shared_print_pool_local_h (local_h)
);
