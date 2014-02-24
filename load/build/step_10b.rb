require 'hathilog';
require 'hathidb';
require 'hathidata';

log = Hathilog::Log.new();
log.d("Started");

infile = Hathidata::Data.new("holdings_htitem_htmember.multi.$ymd.data");
if !infile.exists? then
  log.e("Failed.");
  raise "Could not find #{infile.path}";
end

db     = Hathidb::Db.new();
conn   = db.get_interactive();
# 3-letter month: jan, feb, mar etc.
curmon = Time.new().strftime("%b").downcase();

# Save holdings_htitem_htmember_jn_#{curmon} as a backup table,
# and something we can use for computing deltas later.

t  = 'holdings_htitem_htmember_jn';
q1 = conn.prepare("RENAME TABLE #{t} TO #{t}_#{curmon}");
q2 = conn.prepare("CREATE TABLE #{t} LIKE #{t}_#{curmon}");
q3 = conn.prepare("LOAD DATA LOCAL INFILE '#{infile.path}' INTO TABLE #{t}");

qx = 1;
[q1, q2, q3].each do |qn|
  log.d("Running query #{qx}");
  log.d(qn);
  qn.execute();
  qx += 1;
end

conn.close();
log.d("Finished");
