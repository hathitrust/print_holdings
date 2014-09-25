require 'hathidb';
require 'hathilog';

log  = Hathilog::Log.new();
log.d("Started");

q1   = "SELECT access_count AS ac, COUNT(*) AS c FROM holdings_htitem_htmember_jn GROUP BY access_count LIMIT 5";
q2   = "UPDATE holdings_htitem_htmember_jn SET access_count = 0 WHERE access_count IS NULL";
q3   = "SELECT COUNT(*) AS c FROM holdings_htitem_htmember_jn WHERE access_count IS NULL";
db   = Hathidb::Db.new();
conn = db.get_conn();

log.d(q1);
conn.query(q1) do |row|
  log.d([row['ac'], row['c']].join("\t"));
end

log.d(q2);
conn.update(q2);

log.d(q3);
conn.query(q3) do |row|
  log.d(row['c']);
  if row['c'].to_i > 0 then
    log.e("Expected 0, got #{row['c'].to_i}");
  end
end

conn.close();
log.d("Done");
