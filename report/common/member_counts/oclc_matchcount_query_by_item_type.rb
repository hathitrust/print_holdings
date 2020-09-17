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
# Augmented with group-by item_type.
query_by_type = conn.prepare(
  %w[
    SELECT item_type, COUNT(distinct ho.oclc) AS numoclc
    FROM holdings_memberitem AS hm, holdings_htitem_oclc AS ho
    WHERE ho.oclc = hm.oclc and member_id = ? GROUP BY item_type
  ].join(' ')
);

# The original query. Because count distinct in groups makes it so you can't just add them together,
# because of course there is always some overlap (e.g. where one oclc is both mono and multi)
query_no_type = conn.prepare(
  %w[
    SELECT COUNT(distinct ho.oclc) AS numoclc
    FROM holdings_memberitem AS hm, holdings_htitem_oclc AS ho
    WHERE ho.oclc = hm.oclc and member_id = ?
  ].join(' ')
);

Hathidata.write('oclc_matchcounts_$ymd.tsv') do |hdout|
  hdout.file.sync = true;
  hdout.file.puts %w[member_id total mono multi serial].join("\t");
  log.d(Hathiquery.get_all_members);
  conn.query(Hathiquery.get_all_members) do |mid_row|
    m = mid_row[:member_id];
    by_type = {
      'total'  => 0,
      'mono'   => 0,
      'multi'  => 0,
      'serial' => 0,
    };
    query_by_type.query(m) do |cnt_row|
      item_type = cnt_row[:item_type];
      count     = cnt_row[:numoclc];      
      log.d("#{m} #{item_type} #{count}");
      by_type[item_type] = count;      
    end
    query_no_type.query(m) do |cnt_row|
      count = cnt_row[:numoclc];      
      log.d("#{m} total #{count}");
      by_type['total'] = count;      
    end

    hdout.file.puts [
      m.to_s,
      by_type['total'],
      by_type['mono'],
      by_type['multi'],
      by_type['serial']
    ].join("\t");
  end
end

log.d("Done");
conn.close();
