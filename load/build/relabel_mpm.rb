require 'hathidb';
require 'hathilog';

=begin

From the monthlies:

8a)
Assign 'multi' to additional records in memberitem.
This step changes the member-submitted type designation to 'multi'
f0r items that are associated with multipart clusters.

This essentially maps the HathiTrust multi designation onto those items.
Since not all members submit multi files, items that they submit as
singlepart or serial may actually correspond to HathiTrust multis,
so we need to do this.  Forgetting this step will result in no item type
diffs in the MemberCounts table generated later.

mysql> UPDATE holdings_memberitem SET item_type = 'multi' WHERE
oclc in (SELECT oclc FROM holdings_cluster_oclc, holdings_cluster
WHERE holdings_cluster.cluster_id = holdings_cluster_oclc.cluster_id
AND holdings_cluster.cluster_type = 'mpm');

=end


db   = Hathidb::Db.new();
conn = db.get_conn();
log  = Hathilog::Log.new({:file_name => 'relabel_mpm.log'});

sql = %W<
UPDATE
holdings_memberitem
SET
item_type = 'multi'
WHERE
oclc IN (
    SELECT
    oclc
    FROM
    holdings_cluster_oclc AS hco,
    holdings_cluster      AS hc
    WHERE
    hc.cluster_id = hco.cluster_id
    AND
    hc.cluster_type = 'mpm'
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
