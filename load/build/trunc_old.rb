require 'hathidb';
require 'hathilog';

# Part of step_99_cleanup.sh

log  = Hathilog::Log.new();
log.d("Started");

db   = Hathidb::Db.new();
conn = db.get_conn();

# Check COUNT(*) before and after TRUNCATE, on these tables:
ts = [
      "holdings_htitem_htmember_jn_old"
     ];

ts.each do |t|
  sel_q = "SELECT COUNT(*) AS c FROM #{t}";
  log.d(sel_q);
  conn.query(sel_q) do |row|
    log.d(row[:c])
  end

  tru_q = "TRUNCATE #{t}";
  log.d(tru_q);
  conn.update(tru_q);

  log.d(sel_q);
  conn.query(sel_q) do |row|
    log.d(row[:c])
  end
end

log.d("Finished");
