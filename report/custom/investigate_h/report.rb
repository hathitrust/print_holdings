# Find the clusters containing volume_ids with more than 1 distinct value for H.
sql = %w{
    SELECT t1.cluster_id, COUNT(DISTINCT t2.H) AS ch
    FROM holdings_cluster_htitem_jn AS t1
    JOIN holdings_htitem_H          AS t2 ON (t1.volume_id = t2.volume_id)
    GROUP BY t1.cluster_id HAVING ch >= 2
}.join(' ');

=begin

mysql> select * from holdings_cluster_htitem_jn where cluster_id = 7312645;
+------------+---------------------+
| cluster_id | volume_id           |
+------------+---------------------+
|    7312645 | wu.89099014813      |
|    7312645 | yale.39002088548590 |
+------------+---------------------+
2 rows in set (0.00 sec)

mysql> select * from holdings_htitem_H where volume_id in ('wu.89099014813', 'yale.39002088548590');
+---------------------+------+
| volume_id           | H    |
+---------------------+------+
| wu.89099014813      |    3 |
| yale.39002088548590 |    4 |
+---------------------+------+


    SELECT t1.cluster_id, COUNT(DISTINCT t2.H) AS ch, COUNT(DISTINCT t1.volume_id) AS cv
    FROM holdings_cluster_htitem_jn AS t1
    JOIN holdings_htitem_H          AS t2 ON (t1.volume_id = t2.volume_id)
    GROUP BY t1.cluster_id HAVING (cv >= 2 AND ch = 1)

1388859 rows... so it is MOSTLY right.

=end
