SELECT "Dropping table" AS info;
DROP TABLE IF EXISTS oclc_concordance;

SELECT "Creating table" AS info;;
CREATE TABLE oclc_concordance (
       variant  INT UNSIGNED NOT NULL,
       resolved INT UNSIGNED NOT NULL,
       INDEX oclc_concordance_variant  USING HASH (variant),
       INDEX oclc_concordance_resolved USING HASH (resolved)
);

-- Load table with load_oclc_concordance.rb

-- Returns resolved oclc if found, otherwise returns input.
-- SELECT resolve_oclc(1000624734) --> 9939612
-- SELECT resolve_oclc(666)        --> 666
SELECT "Creating function" 
DROP FUNCTION IF EXISTS resolve_oclc;
DELIMITER //
CREATE FUNCTION resolve_oclc(unresolved_oclc INT) RETURNS INT
  BEGIN
  DECLARE resolved_oclc INT DEFAULT 0;
  SELECT resolved INTO resolved_oclc FROM oclc_concordance WHERE variant = unresolved_oclc;
  IF (resolved_oclc = 0) THEN
     SET resolved_oclc := unresolved_oclc;
  END IF;
  RETURN resolved_oclc;
END //
DELIMITER ;
