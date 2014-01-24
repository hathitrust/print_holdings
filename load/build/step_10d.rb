require 'hathilog';
require 'hathidb';

log = Hathilog::Log.new();
log.d("Started.");

db   = Hathidb::Db.new();
conn = db.get_conn();

# Some sanity check queries.
results = [];
queries  = [
            "SELECT COUNT(*) AS c FROM holdings_htitem_htmember_jn where copy_count IS NULL",
            "SELECT COUNT(*) AS c FROM holdings_htitem_htmember_jn",
            "SELECT COUNT(DISTINCT volume_id) AS c FROM holdings_htitem_htmember_jn",
            "SELECT COUNT(DISTINCT volume_id) AS c FROM holdings_htitem",
            "SELECT COUNT(DISTINCT volume_id) AS c FROM holdings_htitem_oclc",
            "SELECT COUNT(DISTINCT volume_id) AS c FROM holdings_htitem WHERE LENGTH(oclcs) > 0",
           ];

queries.each do |q|
  log.d(q);
  conn.query(q) do |res|
    log.d(" -- Result : #{res[:c]}\n");
    results << res[:c];
  end
end

if results[0] == 0 then
  log.i("Result 0 checks out.");
else
  log.w("#{queries[0]} should be 0, was #{results[0]}");
end

conn.close();
log.d("Finished.");
