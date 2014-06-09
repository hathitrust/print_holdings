-- Playing with procedures in MySQL, first step is to be able to
-- loop over a query and do something with the results.
DELIMITER //

DROP PROCEDURE IF EXISTS holdings_cluster_htmember_jn_proc;

CREATE PROCEDURE holdings_cluster_htmember_jn_proc()
BEGIN
    -- These declares MUST come FIRST.
    DECLARE v_member_id VARCHAR(20); -- The variables you are going to use.
    DECLARE done INT DEFAULT FALSE;  -- A stop-variable
    DECLARE curse CURSOR FOR         -- A cursor that gets the data you loop over.
	select distinct member_id from holdings_memberitem order by member_id;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE; -- A stopper.

    -- Empty it.
    TRUNCATE holdings_cluster_htmember_jn;

    OPEN curse; -- Open up cursor and start loop
    the_loop: LOOP

    FETCH curse INTO v_member_id; -- The cursor instantiates the variables.
    IF done THEN
        LEAVE the_loop; -- We break if the handler tells us to.
    END IF;

    SELECT v_member_id AS processing_member, sysdate() AS started;

    -- perhaps create 2 temporary tables,
    -- 1: distinct oclc, cluster_id from holdings_cluster_oclc
    -- 2: distinct oclc, member_id from holdings_memberitem

    INSERT INTO holdings_cluster_htmember_jn (cluster_id, member_id)
    SELECT DISTINCT cluster_id, member_id FROM holdings_cluster_oclc, holdings_memberitem
    WHERE holdings_cluster_oclc.oclc = holdings_memberitem.oclc AND member_id = v_member_id;

    SELECT v_member_id AS processing_member, sysdate() AS finished;

    END LOOP the_loop; -- end loop and close cursor
    CLOSE curse;
END //

DELIMITER ;