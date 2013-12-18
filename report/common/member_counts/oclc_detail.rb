require 'hathidb';
db = Hathidb::Db.new();
dbh = db.get_conn(); 

# Get number of distinct OCLC numbers per member.
# Not actually used, this was a mistake resulting from bad reading of the monthly.

outer_query = "SELECT DISTINCT member_id FROM holdings_memberitem";
inner_query = dbh.prepare("SELECT count(distinct oclc) AS numoclc FROM holdings_memberitem WHERE member_id = ?");

File.open('unique_counts.tsv', 'w:utf-8') do |uc|
  dbh.query(outer_query) do |mid_row|
    m = mid_row[:member_id];
    inner_query.query(m) do |cnt_row|
      count = cnt_row[:numoclc];
      uc.puts [m.to_s, count.to_s].join("\t");
    end
  end
end
