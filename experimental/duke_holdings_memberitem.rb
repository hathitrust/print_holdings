require 'hathidb';
require 'hathilog';
require 'hathidata';

db   = Hathidb::Db.new();
conn = db.get_conn();
log  = Hathilog::Log.new();
hdf  = Hathidata::Data.new('duke_holdings_memberitem.txt').open('w');

q1 = "select oclc from holdings_memberitem where member_id = 'duke'";
log.d(q1);
i = 0;
conn.query(q1) do |res|
  i += 1;
  hdf.file.puts res['oclc'];
  if i % 100000 == 0 then
    log.d(i)
  end
end
log.i('closing up shop...');
hdf.close();
conn.close();
