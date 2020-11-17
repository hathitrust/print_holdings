require 'hathidata';
require 'hathidb';
require 'hathilog';

# set up
log  = Hathilog::Log.new();
db   = Hathidb::Db.new();
conn = db.get_conn();

# count
count_sql = "SELECT COUNT(*) FROM oclc_concordance";
puts "count before:";
conn.query(count_sql) do |row|
  puts row.to_a.join("\t");
end

# update
hd   = Hathidata::Data.new("oclc_concordance/concordance_1_ne_2.txt");
q    = "LOAD DATA LOCAL INFILE '#{hd.path.to_s}' INTO TABLE oclc_concordance";
log.d(q);
conn.execute(q);

# count
puts "after:";
conn.query(count_sql) do |row|
  puts row.to_a.join("\t");
end

# shut down
conn.close();
log.d("Finished");
