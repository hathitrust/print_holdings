require 'hathidata';
require 'hathidb';
require 'hathilog';

require_relative 'reformat_member_report';

=begin

Provides some data for the "Match Detail" tab in the "Member Counts" report.

=end

log  = Hathilog::Log.new();
log.d("Started");
db   = Hathidb::Db.new();
conn = db.get_conn();
hd1  = Hathidata::Data.new("match_detail_1_$ymd.tsv").open('w');

sql = %W<
SELECT
  member_id,
  access,
  item_type,
  SUM(H_count) AS c
FROM
  holdings_H_counts
GROUP BY
  member_id,
  access,
  item_type
>.join(' ');

log.d(sql);
# This gets our intermediate format.
cols = [:member_id, :access, :item_type, :c];
conn.query(sql) do |row|
  hd1.file.puts(cols.map{|c| row[c]}.join("\t"));
end

hd1.close();
conn.close();

# Call method in reformat_member_report.rb.
# This gets our final, usable format.
Hathidata.write("match_detail_2_$ymd.tsv") do |hd2|
  build_data_hash2(hd1.path).each_pair do |k, v|
    hd2.file.puts(v.to_s);
  end
end

log.d("Finished");
