require 'hathidata';
require 'hathidb';
require 'hathilog';

def get_loadfile (log)
  loadfile = nil;
  if ARGV.length > 0 then
    loadfile = ARGV.shift;
    log.d("Got filename #{loadfile} through ARGV");
  else
    loadfile = 'hathi_full_$ymd.data';
    log.d("Assuming #{loadfile}");
  end

  hd = Hathidata::Data.new(loadfile);
  if hd.exists? then
    log.d("#{hd.path} exists.");
    return hd.path.to_s;
  else
    log.e("Could not figure out which file to use.");
  end
  return nil;
end

if $0 == __FILE__ then
  log = Hathilog::Log.new();
  log.d("Started");

  db   = Hathidb::Db.new();
  conn = db.get_conn();

  loadfile = get_loadfile(log);

  if loadfile == nil then
    raise "Failed";
  end

  check_sql   = "SELECT COUNT(*) AS c FROM holdings_htitem";
  check_query = conn.prepare(check_sql);

  # Check count before...
  check_query.enumerate do |res|
    log.d("Before... :");
    log.d("#{check_sql} ... #{res[:c]}");
  end

  [
   #"TRUNCATE holdings_htitem",
   #"LOAD DATA LOCAL INFILE '#{loadfile}' INTO TABLE holdings_htitem"
  ].each do |q|
    log.d(q);
    conn.update(q);
  end

  # Check count after.
  check_query.enumerate do |res|
    log.d("After... :");
    log.d("#{check_sql} ... #{res[:c]}");
  end

  conn.close();
  log.d("Finished");
end
