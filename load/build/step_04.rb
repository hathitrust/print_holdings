require 'hathilog';
require 'hathidb';

# Copied from /htapps/pulintz.babel/Code/phdb/bin/update_htitem_multiparts.rb
# and reeducated to comply with the new regime.

# This routine labels as 'multi' additional items associated with multipart
# clusters that don't have any n_enum data.

log  = Hathilog::Log.new();
log.d("Started.");
db   = Hathidb::Db.new();
conn = db.get_conn();

# Preparing the queries.
# select multipart clusters
sql_select_1 = %W<
    SELECT
        DISTINCT(cluster_id) AS cid
    FROM
        holdings_cluster_htitem_jn AS chtij,
        holdings_htitem            AS h
    WHERE
        h.volume_id = chtij.volume_id
        AND
        h.item_type = 'multi'
>.join(' ');

sql_select_2 = %W<
    SELECT
        DISTINCT(volume_id)
    FROM
        holdings_cluster_htitem_jn
    WHERE
        cluster_id = ?
>.join(' ');
query_select_2 = conn.prepare(sql_select_2);

sql_update = %W<
    UPDATE 
        holdings_htitem 
    SET 
        item_type = 'multi' 
    WHERE 
        volume_id = ?
>.join(' ');
query_update = conn.prepare(sql_update);

cluster_count = 0;
log.d(sql_select_1);
log.d(sql_select_2);
log.d(sql_update);
conn.query(sql_select_1) do |row|
  cid = row[:cid];
  # get volume_ids associated with the cluster
  query_select_2.enumerate(cid) do |vrow|
    vid = vrow[0];
    query_update.execute(vid);
  end
  cluster_count += 1;
  if ((cluster_count % 100000) == 0) then
    log.d("#{cluster_count} distinct cluster_ids ...");
  end
end
conn.close();

log.d("Finished");
