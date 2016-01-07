require 'hathidb';
require 'hathilog';
require 'hathidata';
require 'hathiquery';

db   = Hathidb::Db.new();
conn = db.get_conn(); 
log  = Hathilog::Log.new();

# Get number of MATCHING oclc numbers per member.
# Used in the 
# + "Member Counts" report
#  \>>+ "OCLC Detail" sheet
#      \>>+ "Total Matching" column.

log.d("Started");
inner_query = conn.prepare(["SELECT COUNT(distinct ho.oclc) AS numoclc",
                            "FROM holdings_memberitem AS hm, holdings_htitem_oclc AS ho",
                            "WHERE ho.oclc = hm.oclc and member_id = ?"].join(' '));

Hathidata.write('oclc_matchcounts_$ymd.tsv') do |hdout|
  hdout.file.sync = true;
  log.d(Hathiquery.get_all_members);
  conn.query(Hathiquery.get_all_members) do |mid_row|
    m = mid_row[:member_id];
    inner_query.query(m) do |cnt_row|
      count = cnt_row[:numoclc];
      log.d("#{m} #{count}");
      hdout.file.puts [m.to_s, count.to_s].join("\t");
    end
  end
end

log.d("Done");
conn.close();
