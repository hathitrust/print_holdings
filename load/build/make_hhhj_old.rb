# Part of step_00_backup.sh

require 'hathidb';
require 'hathilog';

log = Hathilog::Log.new();
log.d("Started");

db    = Hathidb::Db.new();
conn  = db.get_conn();
table = "holdings_htitem_htmember_jn";

[
 "TRUNCATE TABLE #{table}_old",
 "INSERT INTO #{table}_old (SELECT * FROM #{table})"
].each do |q|
  log.d(q);
  pq = conn.prepare(q);
  pq.execute();
end

conn.close();
log.d("Finished");
