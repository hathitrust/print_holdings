# Part of step_00_backup.sh
# Copies the current holdings_htitem_htmember_jn into holdings_htitem_htmember_jn_old.

require 'hathidb';
require 'hathilog';
require 'hathidata';

log = Hathilog::Log.new();
log.d("Started");

db    = Hathidb::Db.new();
conn  = db.get_conn();
table = "holdings_htitem_htmember_jn";

delete_sql = "TRUNCATE TABLE holdings_htitem_htmember_jn_old";

get_members_sql = "SELECT DISTINCT member_id FROM holdings_htitem_htmember_jn WHERE member_id != ''";
get_members_q   = conn.prepare(get_members_sql);

select_sql = "SELECT volume_id, member_id, copy_count, lm_count, wd_count, brt_count, access_count FROM holdings_htitem_htmember_jn WHERE member_id = ?";
select_q   = conn.prepare(select_sql);

load_sql = "LOAD DATA LOCAL INFILE ? INTO TABLE holdings_htitem_htmember_jn_old";
load_q   = conn.prepare(load_sql);

log.d(delete_sql);
conn.execute(delete_sql);

cols = [:volume_id, :member_id, :copy_count, :lm_count, :wd_count, :brt_count, :access_count];

hdout = nil;
log.d(get_members_sql);
get_members_q.enumerate() do |member_row|
  member_id = member_row[:member_id];
  log.d(select_sql);
  log.d(member_id);
  hdout = Hathidata::Data.new('holdings_htitem_htmember_jn_old.dat').open('w');  
  select_q.enumerate(member_id) do |row|
    hdout.file.puts(cols.map{|x| row[x]}.join("\t"));
  end
  hdout.close();
  log.d(load_q);
  load_q.execute(hdout.path);
end
hdout.delete() if !hdout.nil?;

conn.close();
log.d("Finished");
