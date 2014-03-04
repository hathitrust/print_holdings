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
t      = 'holdings_htitem_htmember_jn';
# 3-letter month: jan, feb, mar etc.
curmon = Time.new().strftime("%b").downcase();

# Save holdings_htitem_htmember_jn_#{curmon} as a backup table,
# and something we can use for computing deltas later.
# Prepare them first so that we fail early.
qs = [
      conn.prepare("RENAME TABLE #{t} TO #{t}_#{curmon}"),
      conn.prepare("CREATE TABLE #{t} LIKE #{t}_#{curmon}"),
      conn.prepare("LOAD DATA LOCAL INFILE '#{infile.path}' INTO TABLE #{t}")
];

qs.each_with_index do |q,i|
  j = i+1;
  log.d("Running query #{j} / #{qs.size}");
  log.d(q);
  q.execute();
end

conn.close();
log.d("Finished");
