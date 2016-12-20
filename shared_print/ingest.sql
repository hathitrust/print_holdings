create table shared_print_commitments (
    id                   bigint(20)  NOT NULL AUTO_INCREMENT,
    member_id            varchar(20) NOT NULL,
    oclc                 bigint(20)  NOT NULL,
    local_id             varchar(50),
    oclc_symbol          varchar(5)  NOT NULL, -- might be different from holdings_htmember.oclc_sym
    local_item_id        varchar(1),           -- what even is this
    local_item_location  varchar(50),
local_shelving_type  ENUM('cloa', 'clca', 'sfca', 'sfcahm', 'sfcaasrs'),
    ht_retention_date 	 DATE,
    ht_retention_flag 	 tinyint(1),
    other_commitment_id  int(11),
    lending_policy 	 ENUM('a', 'b'),
    linkage_info 	 varchar(100),
    ownership_history 	 varchar(100),
    PRIMARY KEY (id),
    INDEX sp_commitment_member_id (member_id),
    INDEX sp_commitment_oclc (oclc)
); 

create table shared_print_other (
    id 	     	   bigint(20)  NOT NULL,
    sp_program     ENUM('test', 'east', 'west', 'north', 'south') NOT NULL,
    retention_date DATE,
    INDEX shared_print_other_id (id)
);
