require 'hathilog';
require 'hathidb';

log = Hathilog::Log.new();
log.d("Started");

db      = Hathidb::Db.new();
conn    = db.get_conn();
results = [];

[
 "SELECT COUNT(DISTINCT volume_id) AS c FROM holdings_htitem_htmember_jn WHERE volume_id NOT IN (SELECT volume_id FROM holdings_htitem)",
 "SELECT COUNT(DISTINCT volume_id) AS c FROM holdings_htitem_htmember_jn",
].each do |q|
  log.d(q);
  conn.query(q) do |res|
    log.d(res[:c]);
    results << res[:c].to_i;
  end
end

if results[0] == 0 then
  log.d("First result is zero, as expected.");
else
  log.w("First result unexpected. Expected 0, got #{results[0]}");
end

if results[1] == 0 then
  log.w("Second result was zero. UNEXPECTED");
else
  log.d("Second result non-zero, as expected.");
end

conn.close();
log.d("Finished");
