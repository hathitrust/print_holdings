require 'hathidata';
require 'hathidb';

# Output shared_print_pool for a member, with added col H.
# Gotta do a bunch of joins because H and volume_id aren't in shared_print_pool.

db        = Hathidb::Db.new();
conn      = db.get_conn();
member_id = ARGV.shift;
outfn     = "reports/shared_print/shared_print_pool_#{member_id}_$ymd.tsv";
hdout     = Hathidata::Data.new(outfn).open('w');

report_sql = %w{
  SELECT DISTINCT spp.member_id, spp.local_oclc, spp.oclc, spp.local_h, hhh.H
  FROM shared_print_pool          AS spp
  JOIN holdings_cluster_oclc      AS hco  ON (spp.oclc = hco.oclc)
  JOIN holdings_cluster           AS hc   ON (hco.cluster_id = hc.cluster_id)
  JOIN holdings_cluster_htitem_jn AS hchj ON (hc.cluster_id  = hchj.cluster_id)
  JOIN holdings_htitem_H          AS hhh  ON (hchj.volume_id = hhh.volume_id)
  WHERE spp.member_id = ? 
  AND hc.cluster_type = 'spm'
  ORDER BY spp.oclc
}.join(' ');

legend = [
  "local_oclc : the OCLC number used by #{member_id} in their latest print holdings submission",
  "oclc : resolved OCLC number",
  "local_h : number of Shared Print members (including #{member_id}) holding the same OCLC number",
  "h : number of HathiTrust members (including Shared Print members) holding the same OCLC number",
  
];
legend.each do |leg|
  hdout.file.puts("#\t#{leg}");
end

hdout.file.puts(%w{member_id local_oclc oclc local_h h}.join("\t"));

report_q = conn.prepare(report_sql);
report_q.enumerate(member_id) do |row|
  hdout.file.puts(row.to_a.join("\t"));
end

hdout.close();
