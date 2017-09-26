require 'hathidata';
require 'hathidb';
require 'hathilog';

# List the oclcs of spms that are in HathiTrust but not in Shared Print.
# Might need ruby -J-Xmx8000m

db    = Hathidb::Db.new();
conn  = db.get_conn();
log   = Hathilog::Log.new();
hdout = Hathidata::Data.new("reports/shared_print/not_in_spp_$ymd.tsv").open('w');

expand_sql = "SELECT oclc_y FROM oclc_resolution WHERE oclc_x = ?";
expand_q   = conn.prepare(expand_sql);
ocn_uniq   = {};

log.d("Getting oclcs from hathi");
main_sql = %w{
  SELECT DISTINCT COALESCE(o.oclc_x, hco.oclc) AS ocn
  FROM holdings_cluster_oclc  AS hco
  JOIN holdings_cluster       AS hc  ON (hco.cluster_id = hc.cluster_id)
  LEFT JOIN oclc_resolution   AS o   ON (hco.oclc = o.oclc_y)
  LEFT JOIN shared_print_pool AS spp ON (COALESCE(o.oclc_x, hco.oclc) = spp.resolved_oclc)
  WHERE spp.resolved_oclc IS NULL AND hc.cluster_type = 'spm'
  ORDER BY hco.oclc
}.join(' ');
main_q = conn.prepare(main_sql);
main_q.enumerate() do |row|
  ocn = row[:ocn].to_i;
  ocn_uniq[ocn] = true;
  expand_q.enumerate(ocn) do |ocns|
    ocn_y = ocns[:oclc_y];
    ocn_uniq[ocn_y] = true;
  end
end

ocn_uniq.keys.sort.each do |k|
  hdout.file.puts k;
end

hdout.close();
