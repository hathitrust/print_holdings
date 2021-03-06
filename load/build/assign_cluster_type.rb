require 'hathidb';
require 'hathilog';

=begin

Part of step 6.
MW rewrite of /htapps/pete.babel/Code/phdb/bin/assign_cluster_type.rb. 
2014-01-07.

Main changes:
-- use hathidb
-- prepared queries
-- statements terminated by ;

=end

def assign_cluster_type()
  # Set up connection and prepare queries.

  db   = Hathidb::Db.new();
  conn = db.get_conn();
  log  = Hathilog::Log.new();
  log.i("Starting to assign cluster types.");

  conn.fetch_size = 50000;
  cluster_count   = 0;
  sel_cluster_id_sql = "SELECT cluster_id FROM holdings_cluster";

=begin

A prettier & more compact version of sel_item_type_sql would be:

SELECT GROUP_CONCAT(DISTINCT h.item_type ORDER BY h.item_type SEPARATOR "/")
FROM holdings_htitem            AS h
JOIN holdings_cluster_htitem_jn AS hchj ON (h.volume_id  = hchj.volume_id)
JOIN holdings_cluster           AS c    ON (c.cluster_id = hchj.cluster_id)
WHERE c.cluster_id = ?

... then we just gotta replace ctype->ckey strings: serial->ser, mono->mpm, multi->mpm

=end
  
  sel_item_type_sql = %w<
    SELECT DISTINCT
      item_type 
    FROM 
      holdings_htitem            AS h,
      holdings_cluster_htitem_jn AS chtij,
      holdings_cluster           AS c
    WHERE
      c.cluster_id    = chtij.cluster_id
      AND
      chtij.volume_id = h.volume_id
      AND
      c.cluster_id    = ?
  >.join(' ');
  sel_item_type_q = conn.prepare(sel_item_type_sql);

  update_sql = %w<
    UPDATE holdings_cluster 
    SET    cluster_type = ? 
    WHERE  cluster_id   = ?
  >.join(' ');
  update_q = conn.prepare(update_sql);

  # Get cluster_ids.
  conn.enumerate(sel_cluster_id_sql).each_slice(50000) do |slice|
    slice.each do |row|
      cid = row[0];
      # grab all item_types for items in the cluster
      ctypes = [];

      sel_item_type_q.execute(cid).each do |ctype|
        ckey = '';
        case ctype[0]
        when 'mono'
          ckey = 'spm';
        when 'multi'
          ckey = 'mpm';
        when 'serial'
          ckey = 'ser';
        else
          log.w("#{ctype[0]} type unknown.");
          ckey = 'spm';
        end
        ctypes << ckey;
      end
      typekey = ctypes.sort.join('/');
      # update
      update_q.execute(typekey, cid);

      cluster_count += 1;
      if ((cluster_count % 50000) == 0) then
        log.i("#{cluster_count}...");
      end
    end
  end
  log.i("done assigning cluster types.");
end

if $0 == __FILE__ then
  assign_cluster_type();
end
