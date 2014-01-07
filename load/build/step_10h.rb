require 'hathidb';
require 'hathilog';

q1 = "SELECT access_count AS ac, count(*) AS c FROM holdings_htitem_htmember_jn GROUP BY access_count LIMIT 5";
q2 = "UPDATE holdings_htitem_htmember_jn SET access_count = 0 WHERE access_count IS NULL";
q3 = "SELECT count(*) AS c FROM holdings_htitem_htmember_jn WHERE access_count IS NULL";

log  = Hathilog::Log.new();
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
  if row['c'].to_i > 0 then
    log.e("Expected 0, got #{row['c'].to_i}");
  end
end

log.d("Done");

conn.close();
log.close();
