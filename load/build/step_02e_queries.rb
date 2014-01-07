require 'hathidb';
require 'hathilog';
require 'pathname';

log  = Hathilog::Log.new();

log.d("Started");

if ARGV.length < 1 then
  log.e("Require a path to an infile as 1st arg.");  
  exit(1);
end

infile_path = ARGV.shift;

if !Pathname.new(infile_path).exist?() then
  log.e("Require a VALID PATH to an infile as 1st arg.");  
  exit(1);
end

if !Pathname.new(infile_path).readable?() then
  log.e("Require a path to a READABLE infile as 1st arg.");  
  exit(1);
end

db   = Hathidb::Db.new();
conn = db.get_conn();

qs = [
      "TRUNCATE holdings_htitem_oclc_tmp",
      "LOAD DATA LOCAL INFILE '#{infile_path}' INTO TABLE holdings_htitem_oclc_tmp",
      "INSERT IGNORE INTO holdings_htitem_oclc SELECT * FROM holdings_htitem_oclc_tmp",
];

qs.each do |q|
  log.i(q);
  conn.execute(q);
end

log.d("Finished");
