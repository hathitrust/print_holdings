CREATE TABLE investigate_utexas_tmp (
       volume_id varchar(50) NOT NULL,
       utexas_nov tinyint DEFAULT 0,
       utexas_dec tinyint DEFAULT 0,
       berkeley_nov tinyint DEFAULT 0,
       berkeley_dec tinyint DEFAULT 0,
       nrlf_nov tinyint DEFAULT 0,
       nrlf_dec tinyint DEFAULT 0,
       h_nov tinyint DEFAULT 0,
       h_dec tinyint DEFAULT 0,
       PRIMARY KEY (volume_id)
);
