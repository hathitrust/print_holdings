require 'hathidb';
require 'hathilog';

# Part of step_16.

db   = Hathidb::Db.new();
conn = db.get_conn();
log  = Hathilog::Log.new();
log.d("Started");

qs = [
      "TRUNCATE holdings_htitem_htmember_jn_dev",
      [
       "INSERT INTO holdings_htitem_htmember_jn_dev",
       "(SELECT * FROM holdings_htitem_htmember_jn)"
      ].join(' ')
     ];

qs.each do |q|
  log.d(q);
  conn.execute(q);
end

conn.close();
log.d("Finished");
