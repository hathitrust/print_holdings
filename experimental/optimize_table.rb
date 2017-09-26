require 'hathidb';
require 'hathilog';

log = Hathilog::Log.new();
log.d("Started");
db = Hathidb::Db.new();
cx = db.get_conn();
sql = "OPTIMIZE TABLE holdings_deltas";
log.d(sql);
x = cx.execute(sql);
x.each do |y|
  puts y.to_a.join("|");
end
log.d("Finished");
