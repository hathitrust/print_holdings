require 'hathilog';
require 'hathidb';

# MW Jan 2014. Copied from:
# /htapps/pete.babel/Code/phdb/bin/update_htitem_multiparts.rb
# ... and reeducated to comply with the new regime.

# This routine labels as 'multi' additional items associated with
# multipart clusters that don't have any n_enum data.

log  = Hathilog::Log.new();
log.d("Started.");
db   = Hathidb::Db.new();
conn = db.get_conn();

# Preparing the queries.

sql_check = %w<
  SELECT item_type, COUNT(*) AS c FROM holdings_htitem GROUP BY item_type
>.join(' ');
query_check = conn.prepare(sql_check);

# select multipart clusters
sql_select_1 = %w<
SELECT
    DISTINCT(cluster_id) AS cid
FROM
     holdings_cluster_htitem_jn AS chtij
JOIN holdings_htitem            AS h ON (h.volume_id = chtij.volume_id)
WHERE
    h.item_type = 'multi'
>.join(' ');

sql_select_2 = %w<
  SELECT DISTINCT(volume_id) FROM holdings_cluster_htitem_jn WHERE cluster_id = ?
>.join(' ');
query_select_2 = conn.prepare(sql_select_2);

sql_update = %w<
  UPDATE holdings_htitem SET item_type = 'multi' WHERE volume_id = ?
>.join(' ');
query_update = conn.prepare(sql_update);

log.d("Checking counts before...");
log.d(sql_check);
query_check.enumerate() do |row|
  log.d("#{row[:item_type]}\t#{row[:c]}");
end

cluster_count = 0;
log.d(sql_select_1);
log.d("\t" + sql_select_2);
log.d("\t\t" + sql_update);
conn.query(sql_select_1) do |row|
  cid = row[:cid];
  # get volume_ids associated with the cluster
  log.d("Updating cluster #{cid} to item_type=multi");
  query_select_2.enumerate(cid) do |vrow|
    vid = vrow[0];
    query_update.execute(vid);
  end
  cluster_count += 1;
  if ((cluster_count % 100000) == 0) then
    log.d("#{cluster_count} distinct cluster_ids ...");
  end
end

log.d("Checking counts after...");
log.d(sql_check);
query_check.enumerate() do |row|
  log.d("#{row[:item_type]}\t#{row[:c]}");
end

conn.close();
log.d("Finished");
log.close();
