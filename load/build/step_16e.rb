require 'hathilog';
require 'hathidb';

def check_main_table (conn, log)
  q = "SELECT COUNT(*) AS c FROM holdings_htitem_htmember";
  log.d(q);
  conn.query(q) do |res|
    log.d(res[:c]);
  end
end

log = Hathilog::Log.new({:file_name => 'builds/current/step_16e.log'});
log.d("Started");

db = Hathidb::Db.new();

### PROD

log.d("Getting prod DB credentials... ");
pi_conn = db.get_prod_interactive();

check_main_table(pi_conn, log);

# Jul 22 2020: adding, somewhat temporarily, for ETAS-hack purposes, the following:
# "INSERT IGNORE INTO holdings_htitem_htmember_jn_dev (SELECT volume_id, member_id, 1,0,0,0,0 FROM holdings_newitems)",
# "TRUNCATE holdings_newitems",

[
  "DROP TABLE IF EXISTS holdings_htitem_htmember_old",

  "INSERT IGNORE INTO holdings_htitem_htmember_jn_dev (SELECT volume_id, member_id, 1,0,0,0,0 FROM holdings_newitems)",
  "ALTER TABLE holdings_htitem_htmember_jn_dev ENGINE=innodb",
  "TRUNCATE holdings_newitems",

  "RENAME TABLE holdings_htitem_htmember TO holdings_htitem_htmember_old",
  "RENAME TABLE holdings_htitem_htmember_jn_dev TO holdings_htitem_htmember",
  "DROP TABLE holdings_htitem_htmember_old",
  "CREATE TABLE holdings_htitem_htmember_jn_dev LIKE holdings_htitem_htmember",
].each do |q|
  log.d("/*PROD:*/ " + q);
  pi_conn.execute(q);
end

check_main_table(pi_conn, log);

pi_conn.close();

### DEV

log.d("Getting dev DB credentials... ");
di_conn = db.get_interactive();
[
 "TRUNCATE holdings_htitem_htmember_jn_dev",
].each do |q|
  log.d("/*DEV:*/ " + q);
  di_conn.execute(q);
end
di_conn.close();

log.d("Finished");
