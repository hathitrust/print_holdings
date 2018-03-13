require 'hathidata';
require 'hathidb';

# https://tools.lib.umich.edu/jira/browse/HTP-1050
# b) Details by library (for all HT members, one library per row) (these are total numbers, not individual title records)
#    member_ID
#    Total SPM (same as Member Count report)
#    Matching SPM(same as Member Count report)
#    Total commitments (0 for non-SP libraries)
#    Committed Distinct OCNs (0 for non-SP libraries)
#    Non-SP Distinct OCNs (this is the new data)

db   = Hathidb::Db.new();
conn = db.get_conn();

# Do this little dance to get member_ids that are either in ht or sp
# (since now the latter aren't guaranteed to be a subset of the former).
member_ids = [];
get_ph_member_ids = "SELECT member_id FROM holdings_htmember";
conn.query(get_ph_member_ids) do |row|
  member_ids << row[:member_id];
end
get_sp_member_ids = "SELECT DISTINCT member_id FROM shared_print_commitments";
conn.query(get_sp_member_ids) do |row|
  member_ids << row[:member_id];
end
member_ids = member_ids.sort.uniq;

# Total spm:
total_spm_sql = "SELECT COUNT(*) AS c FROM holdings_memberitem WHERE item_type = 'mono' AND member_id = ?";
total_spm_q   = conn.prepare(total_spm_sql);

# Matching spm:
matching_spm_sql = %w<
  SELECT COUNT(DISTINCT hhj.volume_id) AS c
  FROM   holdings_htitem_htmember_jn AS hhj, holdings_htitem AS h
  WHERE  hhj.volume_id = h.volume_id AND h.item_type = 'mono' AND hhj.member_id = ?
>.join(" ");
matching_spm_q = conn.prepare(matching_spm_sql);

# Total commitments:
total_commitments_sql = "SELECT COUNT(*) AS c FROM shared_print_commitments WHERE member_id = ?";
total_commitments_q = conn.prepare(total_commitments_sql);

# Distinct oclcs committed:
distinct_oclc_commitments_sql = "SELECT COUNT(DISTINCT resolved_oclc) AS c FROM shared_print_commitments WHERE member_id = ?";
distinct_oclc_commitments_q = conn.prepare(distinct_oclc_commitments_sql);

# Distinct non-sp oclcs:
distinct_non_sp_oclc_sql = %w<
  SELECT COUNT(DISTINCT hco.oclc) AS c
  FROM holdings_cluster             AS hc
  JOIN holdings_cluster_oclc        AS hco  ON (hc.cluster_id = hco.cluster_id)
  JOIN holdings_cluster_htmember_jn AS hchj ON (hc.cluster_id = hchj.cluster_id)
  LEFT JOIN shared_print_pool       AS spp  ON (hco.oclc = spp.resolved_oclc)
  WHERE hc.cluster_type = 'spm' AND hchj.member_id = ? AND spp.resolved_oclc IS NULL
>.join(' ');
distinct_non_sp_oclc_q = conn.prepare(distinct_non_sp_oclc_sql);

# distinct_non_sp_h_1
# ... for each member_ID, count distinct_non_sp OCNs where h=1
# (i.e. this library only). Calculate this for all member IDs.
# Our existing summary counts show that >933,000 non SP OCNs
# are held by only one HT library, and this new count will
# show how many of those are in a non-Retention Library.
distinct_non_sp_h_1_sql = %w<
  SELECT COUNT(DISTINCT hco.oclc) AS c
  FROM holdings_cluster              AS hc
  JOIN holdings_cluster_oclc         AS hco ON (hc.cluster_id = hco.cluster_id)
  JOIN holdings_cluster_htmember_jn  AS hcm ON (hc.cluster_id = hcm.cluster_id)
  JOIN holdings_cluster_htitem_jn    AS hci ON (hc.cluster_id = hci.cluster_id)
  JOIN holdings_htitem_H             AS hhh ON (hci.volume_id = hhh.volume_id)
  LEFT JOIN shared_print_commitments AS spc ON (hco.oclc = spc.resolved_oclc)
  WHERE hc.cluster_type = 'spm' 
  AND hcm.member_id = ? 
  AND spc.resolved_oclc IS NULL
  AND hhh.H = 1
>.join(' ');
distinct_non_sp_h_1_q = conn.prepare(distinct_non_sp_h_1_sql);

hdout = Hathidata::Data.new("reports/shared_print/1050b_$ymd.tsv").open('w');
# Header
hdout.file.puts(
  %w[
    member_id total_spm 
    matching_spm total_commitments 
    distinct_oclc_commitments 
    distinct_non_sp_oclc
    distinct_non_sp_h_1
  ].join("\t")
);
qs = [
  total_spm_q,
  matching_spm_q,
  total_commitments_q,
  distinct_oclc_commitments_q,
  distinct_non_sp_oclc_q,
  distinct_non_sp_h_1_q
];
member_ids.each do |member_id|
  out = [member_id];
  qs.each do |q|
    q.enumerate(member_id) do |row|
      out << row[:c];
    end
  end
  hdout.file.puts(out.join("\t"));
end

hdout.close();
