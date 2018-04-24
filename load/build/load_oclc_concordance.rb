require 'hathidata';
require 'hathidb';
require 'hathilog';

log  = Hathilog::Log.new();
db   = Hathidb::Db.new();
conn = db.get_conn();
hd   = Hathidata::Data.new("oclc_concordance/concordance_1_ne_2.txt");
q    = "LOAD DATA LOCAL INFILE '#{hd.path.to_s}' INTO TABLE oclc_concordance";

log.d(q);
conn.execute(q);
conn.close();
log.d("Finished");
