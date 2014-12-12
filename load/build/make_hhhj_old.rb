# Part of step_00_backup.sh
# Copies the current holdings_htitem_htmember_jn into holdings_htitem_htmember_jn_old.

require 'hathidb';
require 'hathilog';

log = Hathilog::Log.new();
log.d("Started");

db    = Hathidb::Db.new();
conn  = db.get_conn();
table = "holdings_htitem_htmember_jn";

delete_sql = "TRUNCATE TABLE holdings_htitem_htmember_jn_old";
select_sql = "SELECT volume_id, member_id, copy_count, lm_count, wd_count, brt_count access_count FROM holdings_htitem_htmember_jn";
hdout      = Hathidata::Data.new('holdings_htitem_htmember_jn_old.dat').open('w');
load_sql   = "LOAD DATA LOCAL INFILE ? INTO TABLE holdings_htitem_htmember_jn_old";

log.d(delete_sql);
conn.execute(delete_sql);

log.d(select_sql);
conn.query(select_sql) do |row|
  hdout.file.puts [:volume_id, :member_id, :copy_count, :lm_count, :wd_count, :brt_count, :access_count].map{|x| row[x]}.join("\t");
end
hdout.close();

log.d(load_sql);
load_q = conn.prepare(load_sql);
load_q.execute(hdout.path);

conn.close();
log.d("Finished");
