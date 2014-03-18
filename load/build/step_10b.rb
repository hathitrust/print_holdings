require 'hathilog';
require 'hathidb';
require 'hathidata';

# Log no. rows in t. For before/after comparison.
def t_counts (conn, log, t)
  log.d("Checking counts.");
  q = "SELECT COUNT(*) AS c FROM #{t}";
  log.d(q);
  conn.query(q) do |r|
    log.d(r[:c]);
  end
end

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

t_counts(conn, log, t);

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
  q.execute();
end

t_counts(conn, log, t);

conn.close();
log.d("Finished");
