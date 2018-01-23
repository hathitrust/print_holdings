# https://tools.lib.umich.edu/jira/browse/HTP-1050
require 'hathidata';
require 'hathidb';

# Find all oclcs in HathiTrust that aren't committed and count them by H.

db   = Hathidb::Db.new();
conn = db.get_conn();
q    = %w{
  SELECT DISTINCT x.h, x.oclc FROM (
    SELECT DISTINCT t1.volume_id, t5.H AS h, MIN(t3.oclc) AS oclc
    FROM holdings_htitem_htmember_jn   AS t1
    JOIN holdings_cluster_htitem_jn    AS t2 ON (t1.volume_id  = t2.volume_id)
    JOIN holdings_cluster_oclc         AS t3 ON (t2.cluster_id = t3.cluster_id)
    LEFT JOIN shared_print_commitments AS t4 ON (t3.oclc       = t4.resolved_oclc)
    JOIN holdings_htitem_H             AS t5 ON (t1.volume_id  = t5.volume_id)
    JOIN holdings_cluster              AS t6 ON (t2.cluster_id = t6.cluster_id)
    WHERE t4.resolved_oclc IS NULL
    AND t6.cluster_type = 'spm'
    GROUP BY t1.volume_id, t5.H
  ) AS x
  ORDER BY x.h, x.oclc
}.join(' ');

hdout = Hathidata::Data.new("report_1050a_$ymd.log").open('w');
conn.query(q) do |row|
  hdout.file.puts(row.to_a.join("\t"));
end
hdout.close();
