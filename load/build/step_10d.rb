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

if results[3] > results[4] then
  log.i("Result 3 & 4 check out, 3 is greater than 4.");
else
  log.w("#{queries[3]}\nshould be greater than #{queries[4]}\n... was not (#{results[3]} cmp #{results[4]}).");
end


if results[4] == results[5] then
  log.i("Result 4 & 5 check out, they are the same.");
else
  log.w("#{queries[4]}\nand #{queries[5]}\n... should be the same, was different (#{results[4]} cmp #{results[5]}).");
end

conn.close();
log.d("Finished.");
