create table govdocness (
       oclc BIGINT(20) NOT NULL,
       yea INT NOT NULL DEFAULT 0,
       nea INT NOT NULL DEFAULT 0,
       PRIMARY KEY (oclc)
);
