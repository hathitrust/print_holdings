require 'hathidb';
require 'hathilog';

q0 = "SELECT COUNT(*) AS c FROM holdings_htitem_H";
q1 = "TRUNCATE holdings_htitem_H";
q2 = "INSERT INTO holdings_htitem_H (volume_id, H) SELECT volume_id, COUNT(DISTINCT member_id) FROM holdings_htitem_htmember_jn GROUP BY volume_id";
q3 = q0;

db   = Hathidb::Db.new();
conn = db.get_conn();
log  = Hathilog::Log.new();

# Added after last run, not tried.
log.d(q0);
conn.query(q0) do |res|
  log.d(res[c]);
end

log.d(q1);
conn.update(q1);

log.d(q2);
conn.update(q2);

# Added after last run, not tried.
log.d(q3);
conn.query(q3) do |res|
  log.d(res[c]);
end

log.d("Done");
