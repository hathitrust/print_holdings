create table shared_print_commitments (
    id                   bigint(20)  NOT NULL AUTO_INCREMENT,
    member_id            varchar(20) NOT NULL,
    local_oclc           bigint(20)  NOT NULL,
    resolved_oclc        bigint(20)  NOT NULL, -- not required in input field, but always inserted
    local_id             varchar(50) NOT NULL,
    local_bib_id         varchar(50),
    local_item_id        varchar(50),
    oclc_symbol          varchar(5)  NOT NULL, -- might be different from holdings_htmember.oclc_sym
    local_item_location  varchar(50),
    local_shelving_type  ENUM('cloa', 'clca', 'sfca', 'sfcahm', 'sfcaasrs'),
    ht_retention_date 	 DATE,
    ht_retention_flag 	 tinyint(1),
    other_commitment_id  int(11),
    lending_policy 	 ENUM('blo'),
    scanning_repro_policy ENUM('do not reproduce'),
    ownership_history 	 varchar(100),
    PRIMARY KEY (id),
    INDEX sp_commitment_member_id (member_id),
    INDEX sp_commitment_local_oclc (local_oclc),
    INDEX sp_commitment_resolved_oclc (resolved_oclc)
); 
ALTER TABLE shared_print_commitments ADD committed_date DATE DEFAULT '2017-09-30'; -- added 2018-09-26, Jira # HT-812
create table shared_print_other (
    id 	     	   bigint(20)  NOT NULL,
    sp_program     ENUM('coppul', 'east', 'flare', 'ivyplus', 'mssc', 'recap', 'ucsp', 'viva', 'other', 'fdlp') NOT NULL,
    retention_date DATE,
    indefinite     TINYINT DEFAULT 0,
    INDEX shared_print_other_id (id),
    INDEX shared_print_other_indefinite (indefinite)
);

