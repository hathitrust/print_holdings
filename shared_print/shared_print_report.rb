require 'hathidb';
require 'hathidata';
require 'hathilog';

# Lets a member know which of their holdings are eligible for shared print.

db = Hathidb::Db.new();
conn = db.get_conn();
log = Hathilog::Log.new();

compare_holdings_sql = %w{
    SELECT t1.oclc, hm.status, hm.item_condition, COUNT(DISTINCT t2.member_id) AS calc_h, !ISNULL(hco.oclc) AS in_ht
    FROM shared_print_pool   AS t1
    JOIN shared_print_pool   AS t2 ON (t1.oclc = t2.oclc)
    JOIN holdings_memberitem AS hm ON (t1.holdings_memberitem_id = hm.id)
    LEFT JOIN holdings_cluster_oclc AS hco ON (t1.oclc = hco.oclc)
    WHERE t1.member_id = ?
    GROUP BY t1.oclc, hm.status, hm.item_condition
}.join(' ');

compare_holdings_q = conn.prepare(compare_holdings_sql);

cols = [:oclc, :status, :item_condition, :calc_h, :in_ht];
ARGV.each do |member_id|
  next if member_id.start_with?('--');
  overlap_hdout = Hathidata::Data.new("reports/shared_print_overlap_#{member_id}_$ymd.tsv").open('w');
  overlap_hdout.file.puts(cols.join("\t"));
  compare_holdings_q.enumerate(member_id) do |row|
    overlap_hdout.file.puts(cols.map{|x| row[x]}.join("\t"));
  end
  overlap_hdout.close();
end
