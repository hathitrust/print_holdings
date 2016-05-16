create table oclc_resolution (
    id INT NOT NULL AUTO_increment,
    oclc_x BIGINT(20) NOT NULL,
    oclc_y BIGINT(20) NOT NULL,
    PRIMARY KEY (id),
    INDEX oclc_resolution_oclc_x (oclc_x),
    INDEX oclc_resolution_oclc_y (oclc_y),
    INDEX oclc_resolution_oclc_xy (oclc_x, oclc_y)
); 
