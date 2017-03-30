require 'hathidb';
require 'hathidata';

# Give a tally of how big a slice of the pool each member has
# and how many commitments they have sent us.
# https://wush.net/jira/hathitrust/browse/HTP-1081

db   = Hathidb::Db.new();
conn = db.get_conn();
get_holdings_count        = conn.prepare("SELECT COUNT(*)       AS c FROM holdings_memberitem WHERE member_id = ? AND item_type = 'mono'");
get_pool_count            = conn.prepare("SELECT COUNT(*)       AS c FROM shared_print_pool WHERE member_id = ?");
get_commitment_count      = conn.prepare("SELECT COUNT(*)       AS c FROM shared_print_commitments WHERE member_id = ?");
get_unique_in_pool_count  = conn.prepare("SELECT COUNT(local_h) AS c FROM shared_print_pool WHERE member_id = ? AND local_h = 1");
get_unique_in_hathi_count = conn.prepare(%w{
    SELECT COUNT(hhh.H) AS c
    FROM shared_print_pool          AS spp
    JOIN holdings_cluster_oclc      AS hco  ON (spp.resolved_oclc = hco.oclc)
    JOIN holdings_cluster           AS hc   ON (hco.cluster_id = hc.cluster_id)
    JOIN holdings_cluster_htitem_jn AS hchj ON (hc.cluster_id  = hchj.cluster_id)
    JOIN holdings_htitem_H          AS hhh  ON (hchj.volume_id = hhh.volume_id)
    WHERE spp.member_id = ?
    AND hc.cluster_type = 'spm'
    AND hhh.H = 1
}.join(' '));

hdout = Hathidata::Data.new("reports/shared_print/overview_counts_$ymd.tsv").open('w');
hdout.file.puts(%w[member_id holdings_count pool_count commitment_count percent_pool_committed unique_in_pool unique_in_hathi].join("\t"));

total_holdings_count        = 0;
total_pool_count            = 0;
total_commitment_count      = 0;
total_unique_in_pool_count  = 0;
total_unique_in_hathi_count = 0;

Hathidata.read("shared_print_members.tsv") do |member_id|
  member_id.strip!;
  holdings_count        = 0;
  pool_count            = 0;
  commitment_count      = 0;
  unique_in_pool_count  = 0;
  unique_in_hathi_count = 0;

  get_holdings_count.query(member_id) do |row|
    holdings_count = row[:c].to_i;
  end  
  get_pool_count.query(member_id) do |row|
    pool_count = row[:c].to_i;
  end
  get_commitment_count.query(member_id) do |row|
    commitment_count = row[:c].to_i;
  end
  get_unique_in_pool_count.query(member_id) do |row|
    unique_in_pool_count = row[:c].to_i;
  end
  get_unique_in_hathi_count.query(member_id) do |row|
    unique_in_hathi_count = row[:c].to_i;
  end

  total_holdings_count        += holdings_count;
  total_pool_count            += pool_count;
  total_commitment_count      += commitment_count;
  total_unique_in_pool_count  += unique_in_pool_count;
  total_unique_in_hathi_count += unique_in_hathi_count;
  percent_pool_committed = 'N/A';

  if pool_count > 0 && commitment_count > 0 then
    percent_pool_committed = (100 * (commitment_count.to_f / pool_count)).round(2);
    percent_pool_committed = "#{percent}%"
  end

  hdout.file.puts([member_id, holdings_count, pool_count, commitment_count, percent_pool_committed, unique_in_pool_count, unique_in_hathi_count].join("\t"));
end

total_percent_pool_committed = 'N/A';
if total_pool_count > 0 && total_commitment_count > 0 then
  total_percent_pool_committed = (100 * (total_commitment_count.to_f / total_pool_count)).round(2);
  total_percent_pool_committed = "#{total_percent_pool_committed}%";
end

hdout.file.puts(["TOTAL", total_holdings_count, total_pool_count, total_commitment_count, total_percent_pool_committed, total_unique_in_pool_count, total_unique_in_hathi_count].join("\t"));
hdout.close();
