CREATE TABLE shared_print_augment (
       id        BIGINT(20)  NOT NULL AUTO_INCREMENT,
       member_id ENUM('columbia', 'princeton') NOT NULL,
       local_id  VARCHAR(50) NOT NULL,
       callno    VARCHAR(50) NULL,
       lang      CHAR(3) NULL,
       pub_year  CHAR(4) NULL,
       pub_place CHAR(3) NULL,
       PRIMARY KEY (id),
       INDEX sp_aug_member_id (member_id),
       INDEX sp_aug_local_id (local_id)       
);

-- LOAD DATA LOCAL INFILE '/htapps/mwarin.babel/recap_parser/data/princeton/princeton_augmented_20170530.tsv' INTO TABLE shared_print_augment (member_id, local_id, callno, lang, pub_year, pub_place);
-- LOAD DATA LOCAL INFILE '/htapps/mwarin.babel/recap_parser/data/columbia/columbia_augmented_20170530.tsv' INTO TABLE shared_print_augment (member_id, local_id, callno, lang, pub_year, pub_place);
