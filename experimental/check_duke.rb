require 'hathidb';
require 'hathilog';
require 'hathidata';

log  = Hathilog::Log.new();
db   = Hathidb::Db.new();
conn = db.get_interactive();

hdf = Hathidata::Data.new('duke_volid_jn')
if hdf.exists? then
  log.d("#{hdf.path} already exists");
else
  hdf.open('w');
  q1  = "SELECT DISTINCT volume_id FROM holdings_htitem_htmember_jn WHERE member_id = 'duke'";
  log.d(q1);
  i = 0;
  conn.query(q1) do |row|
    i += 1;
    log.d(i) if i % 100000 == 0;
    hdf.file.puts row['volume_id'];
  end
  hdf.close();
end

hdf_dec = Hathidata::Data.new('duke_volid_jn_dec').open('w');
q2  = "SELECT DISTINCT volume_id FROM mwarin_ht.holdings_htitem_htmember_jn_dec WHERE member_id = 'duke'";
log.d(q2);
i = 0;
conn.query(q2) do |row|
  i += 1;
  log.d(i) if i % 100000 == 0;
  hdf_dec.file.puts row['volume_id'];
end
hdf_dec.close();
