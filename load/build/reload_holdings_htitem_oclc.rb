require 'hathidb';
require 'hathilog';

log = Hathilog::Log.new();
log.d("Started");

db     = Hathidb::Db.new();
conn   = db.get_conn();
infile = ARGV.shift;

raise "Need infile as 1st arg" if infile == nil;

[
 "TRUNCATE holdings_htitem_oclc",
 "LOAD DATA LOCAL INFILE '#{infile}' INTO TABLE holdings_htitem_oclc"
].each do |q|
  log.d(q);
  # conn.update(q);
end

conn.close();
log.d("Finished");
