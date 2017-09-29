require 'hathidb';
require 'hathidata';

conn = Hathidb::Db.new().get_conn();
sql  = "SELECT a.volume_id, a.H from holdings_htitem_H AS a JOIN holdings_htitem AS b ON (a.volume_id = b.volume_id) WHERE b.access = 'allow' ORDER BY a.H ASC";

hdout = Hathidata::Data.new('reports/all_open_volid_H_$ymd.tsv').open('w');
conn.query(sql) do |row|
  hdout.file.puts([:volume_id, :H].map{|x| row[x]}.join("\t"));
end

hdout.close();
hdout.deflate();
conn.close();
