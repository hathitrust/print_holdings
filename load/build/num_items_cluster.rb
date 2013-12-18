require 'hathidb';
require 'hathilog';

=begin

From the monthlies:

6b) 
Calculate num_of_items for each cluster
mysql> UPDATE holdings_cluster SET num_of_items = (select count(volume_id)
from holdings_cluster_htitem_jn where
holdings_cluster.cluster_id = holdings_cluster_htitem_jn.cluster_id);

Query OK, 5442960 rows affected (3 min 18.31 sec)
Rows matched: 5442960  Changed: 5442960  Warnings: 0

=end

db   = Hathidb::Db.new();
conn = db.get_conn();
log  = Hathilog::Log.new({:file_name => 'num_items_cluster.log'});
sql  = %W<
    UPDATE holdings_cluster SET num_of_items = (
        SELECT
        COUNT(hchj.volume_id)
        FROM
        holdings_cluster_htitem_jn AS hchj
        WHERE
        holdings_cluster.cluster_id = hchj.cluster_id
    )
>.join(" ");

log.i('Started');
log.i(sql);

begin
  conn.update(sql);
rescue StandardError => e
  log.e("StandardError");
  log.e("#{e}");
ensure
  conn.close();
end

log.i('Finished');

