# Part of step 02, reloads holdings_htitem_oclc with the contents of
# the given file.

require 'pathname';
require 'hathidb';
require 'hathilog';

log = Hathilog::Log.new();
log.d("Started");

db     = Hathidb::Db.new();
conn   = db.get_conn();
infile = ARGV.shift;

if infile == nil then
  raise "Need infile as 1st arg";
elsif !Pathname.new(infile).exist? then
  raise "infile #{infile} is not a path to an existing file.";
end

[
 "TRUNCATE holdings_htitem_oclc",
 "LOAD DATA LOCAL INFILE '#{infile}' INTO TABLE holdings_htitem_oclc"
].each do |q|
  log.d(q);
  conn.update(q);
end

conn.close();
log.d("Finished");

exit 0;
