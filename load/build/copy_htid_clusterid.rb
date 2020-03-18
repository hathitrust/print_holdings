# holdings_cluster_htitem_jn
require 'hathidb';
require 'hathiconf';
require 'hathilog';
require 'hathienv';

log = Hathilog::Log.new();
log.i("Started");

conf = Hathiconf::Conf.new();

# Dev conf
htrep_dev_host  = conf.get('db_host');
htrep_dev_name  = conf.get('db_name');
htrep_dev_user  = conf.get('db_user');
htrep_dev_pw    = conf.get('db_pw');

# Prod conf
htrep_prod_host = conf.get('prod_db_host');
htrep_prod_name = conf.get('prod_db_name');
htrep_prod_user = conf.get('prod_db_user');
htrep_prod_pw   = conf.get('prod_db_pw');

table_name = "holdings_cluster_htitem_jn"

db = Hathidb::Db.new();
conn = db.get_conn(); # Dev conn.
# How many rows are there in dev?
count_rows = 0;
count_sql  = "SELECT COUNT(*) AS c FROM #{table_name}";
conn.query(count_sql) do |row|
  count_rows = row[:c];
end
conn.close();
log.i("there are #{count_rows} rows to copy from dev to prod");

## truncate in prod, only works if running on prod server
prod_conn = db.get_prod_conn();
trunc_prod_sql = "DELETE FROM #{table_name}";
log.i("deleting prod records: #{trunc_prod_sql}");
trunc_prod_q = prod_conn.prepare(trunc_prod_sql);
trunc_prod_q.execute();

# Do a slice at a time until all the rows are exported.
# One million records per slice.
slice_size = 1000000;
slice_seen = 0;
sleeptime  = 5;

while slice_seen < count_rows do
  
  get_command = %W[
    mysqldump
    -h #{htrep_dev_host}
    -u #{htrep_dev_user}
    -p#{htrep_dev_pw}
    #{htrep_dev_name} #{table_name}
    -w"1 LIMIT #{slice_seen}, #{slice_size}"
    --skip-disable-keys
    --skip-add-locks
    --skip-lock-tables
    --skip-comments
    --skip-set-charset
    --no-create-info
  ].join(' ');

  load_command = %W[
    mysql
    -h #{htrep_prod_host}
    -u #{htrep_prod_user}
    -p#{htrep_prod_pw}
    #{htrep_prod_name}
  ].join(' ')

  pipe_command = "#{get_command} | #{load_command}"  
  log.i(pipe_command);
  system(pipe_command);

  log.i("Napping #{sleeptime} s");
  sleep(sleeptime);
  # Count up.
  slice_seen += slice_size;
end

log.i("Finished");
