require 'hathidata';
require 'hathidb';

# Get the resolved_oclcs that are in the pool but don't have a matching commitment.
# Should only take a couple of minutes to run.

db   = Hathidb::Db.new();
conn = db.get_conn();

hdout_path = "reports/shared_print/in_pool_but_not_committed_$ymd.txt";
hdout = Hathidata::Data.new(hdout_path).open('w');

sql = %w<
  SELECT DISTINCT spp.resolved_oclc AS in_pool, spc.resolved_oclc AS committed
  FROM shared_print_pool AS spp
  LEFT JOIN shared_print_commitments AS spc ON (spp.resolved_oclc = spc.resolved_oclc)
  WHERE spc.resolved_oclc IS NULL
>.join(' ');

conn.query(sql) do |row|
  hdout.file.puts(row[:in_pool]);
end

hdout.close();
