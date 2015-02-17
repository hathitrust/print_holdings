# Touch holdings_dates.last_phdb_build_date

require 'hathienv';
require 'hathidb';
require 'hathilog';

log = Hathilog::Log.new({:file_name => 'builds/current/update_last_phdb_build_date.log'});
log.d("Started");

if !Hathienv::Env.is_prod? then
  log.e("CRASH!");
  raise "This script can only run in prod!";
end

db    = Hathidb::Db.new();
pconn = db.get_prod_conn();

select_sql = "SELECT last_phdb_build_date FROM holdings_dates";

log.d(select_sql);
pconn.query(select_sql) do |row|
  log.d("last_phdb_build_date is #{row[:last_phdb_build_date]} before update");
end

update_sql = "UPDATE holdings_dates SET last_phdb_build_date = SYSDATE()";
log.d(update_sql);
pconn.execute(update_sql);

log.d(select_sql);
pconn.query(select_sql) do |row|
  log.d("last_phdb_build_date is #{row[:last_phdb_build_date]} after update");
end

log.d("Finished");
