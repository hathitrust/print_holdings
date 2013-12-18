require 'hathidb';
require 'hathilog';

db = Hathidb::Db.new();
dbh = db.get_conn(); 

log = Hathilog::Log.new();

# Get number of MATCHING oclc numbers per member.
# Used in the 
# + "Member Counts" report
#  \>>+ "OCLC Detail" sheet
#      \>>+ "Total Matching" column.

outer_query = "SELECT DISTINCT member_id FROM holdings_memberitem";
inner_query = dbh.prepare("SELECT COUNT(distinct ho.oclc) AS numoclc FROM holdings_memberitem AS hm, holdings_htitem_oclc AS ho WHERE ho.oclc = hm.oclc and member_id = ?");

File.open('matching_counts.tsv', 'w:utf-8') do |mc|
  mc.sync = true;
  log.d(outer_query);
  dbh.query(outer_query) do |mid_row|
    m = mid_row[:member_id];
    inner_query.query(m) do |cnt_row|
      count = cnt_row[:numoclc];
      log.d("#{m} #{count}");
      mc.puts [m.to_s, count.to_s].join("\t");
    end
  end
end
