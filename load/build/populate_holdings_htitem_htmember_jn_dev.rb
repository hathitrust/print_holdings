# Part of step_16.

require 'hathidb';
require 'hathilog';
require 'hathidata';

db   = Hathidb::Db.new();
conn = db.get_conn();
conn.java.setAutoReconnect(true); # This does nothing?
log  = Hathilog::Log.new();
log.d("Started");

cols       = %w[volume_id member_id copy_count lm_count wd_count brt_count access_count];
count_sql  = %w[
  SELECT 'holdings_htitem_htmember_jn' AS t, COUNT(*) AS c FROM holdings_htitem_htmember_jn 
  UNION 
  SELECT 'holdings_htitem_htmember_jn_dev' AS t, COUNT(*) AS c FROM holdings_htitem_htmember_jn_dev
].join(" ");
count_q = conn.prepare(count_sql);

trunc_sql  = "TRUNCATE TABLE holdings_htitem_htmember_jn_dev";

get_members_sql = "SELECT distinct member_id FROM holdings_htitem_htmember_jn WHERE member_id != ''";
get_members_q   = conn.prepare(get_members_sql);

select_sql = "SELECT #{cols.join(', ')} FROM holdings_htitem_htmember_jn WHERE member_id = ?";
select_q   = conn.prepare(select_sql);

load_sql   = "LOAD DATA LOCAL INFILE ? INTO TABLE holdings_htitem_htmember_jn_dev";
load_q     = conn.prepare(load_sql);

log.d(count_sql);
count_q.enumerate() do |row|
  puts "#{row[:t]}\t#{row[:c]}";
end

log.d(trunc_sql);
conn.execute(trunc_sql);

hdout = Hathidata::Data.new('holdings_htitem_htmember_jn.dat').open('w');
get_members_q.enumerate do |mrow|
  member_id = mrow[:member_id];
  puts select_sql.sub('?', member_id);
  select_q.enumerate(member_id) do |row|
    hdout.file.puts(cols.map{ |x| row[x] }.join("\t"));
  end
end
hdout.close();

log.d(load_sql);
load_q.execute(hdout.path);

log.d(count_sql);
count_q.enumerate() do |row|
  puts "#{row[:t]}\t#{row[:c]}";
end

hdout.delete();
conn.close();
log.d("Finished");
