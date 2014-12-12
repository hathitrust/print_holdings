# Part of step_16.

require 'hathidb';
require 'hathilog';
require 'hathidata';

db   = Hathidb::Db.new();
conn = db.get_conn();
log  = Hathilog::Log.new();
log.d("Started");

cols       = %w[volume_id member_id copy_count lm_count wd_count brt_count access_count];
count_sql  = %w[
  SELECT 'holdings_htitem_htmember_jn' AS t, COUNT(*) AS c FROM holdings_htitem_htmember_jn 
  UNION 
  SELECT 'holdings_htitem_htmember_jn_dev' AS t, COUNT(*) AS c FROM holdings_htitem_htmember_jn_dev
].join(" ");

trunc_sql  = "TRUNCATE holdings_htitem_htmember_jn_dev";
select_sql = "SELECT #{cols.join(', ')} FROM holdings_htitem_htmember_jn";
load_sql   = "LOAD DATA LOCAL INFILE ? INTO TABLE holdings_htitem_htmember_jn_dev";
hdout      = Hathidata::Data.new('holdings_htitem_htmember_jn.dat').open('w');

log.d(count_sql);
conn.query(count_sql) do |row|
  puts "#{row[:t]}\t#{row[:c]}";
end

log.d(trunc_sql);
conn.execute(trunc_sql);

log.d(select_sql);
conn.enumerate(select_sql) do |row|
  hdout.file.puts(cols.map{ |x| row[x] }.join("\t"));
end
hdout.close();

log.d(load_sql);
load_q = conn.prepare(load_sql);
load_q.execute(hdout.path);

log.d(count_sql);
conn.query(count_sql) do |row|
  puts "#{row[:t]}\t#{row[:c]}";
end

hdout.delete();
conn.close();
log.d("Finished");
