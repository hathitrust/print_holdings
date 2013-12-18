-- ran from mysql command line on kool3, since there is something not kosher in the last line according to the mysql client on punch (newer than koolaid)

RENAME TABLE holdings_htitem_htmember_jn TO mwarin_ht.holdings_htitem_htmember_jn_dec;
CREATE TABLE holdings_htitem_htmember_jn LIKE mwarin_ht.holdings_htitem_htmember_jn_dec;
LOAD DATA LOCAL INFILE '/htapps/mwarin.babel/phdb_scripts/data/holdings_htitem_htmember.multi.20131216.data' INTO TABLE holdings_htitem_htmember_jn;

/*

Query OK, 27696417 rows affected (7 min 33.19 sec)
Records: 27727947  Deleted: 0  Skipped: 31530  Warnings: 0

mysql> select count(*) from holdings_htitem_htmember_jn;
+----------+
| count(*) |
+----------+
| 27696417 |
+----------+

*/