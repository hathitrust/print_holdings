-- If you get "ERROR 1148", restart with --local_infile=1.

use ht_repository;
RENAME TABLE holdings_htitem_htmember_jn TO mwarin_ht.holdings_htitem_htmember_jn_jan;
CREATE TABLE holdings_htitem_htmember_jn LIKE mwarin_ht.holdings_htitem_htmember_jn_jan;
LOAD DATA LOCAL INFILE '/htapps/mwarin.babel/phdb_scripts/data/holdings_htitem_htmember.multi.20140107.data' INTO TABLE holdings_htitem_htmember_jn;