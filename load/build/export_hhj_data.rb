require 'hathidb';
require 'hathiconf';
require 'hathilog';
require 'hathienv';
require 'hathiquery';

# Part of step 16.
# Copied from /htapps/pete.babel/Code/phdb/bin/ and slightly modded.

def check_table (db, log)
  conn = db.get_prod_conn();
  q = "SELECT COUNT(*) AS c FROM holdings_htitem_htmember_jn_dev";
  log.d(q);
  conn.query(q) do |res|
    log.d(res[:c]);
  end
  conn.close();
end

def truncate_table(db, log)
  check_table(db, log);
  conn = db.get_prod_conn();
  # q = "TRUNCATE TABLE holdings_htitem_htmember_jn_dev";
  # Temporary solution, as I cannot TRUNCATE without DROP permission.
  q = "DELETE FROM holdings_htitem_htmember_jn_dev";
  log.d(q);
  conn.execute(q);
  conn.close();
  check_table(db, log);
end

def export_data_files(db, log)
  conn = db.get_conn(); # Dev conn.
  # How many rows are there?
  count_rows = 0;
  count_sql  = "SELECT COUNT(*) AS c FROM holdings_htitem_htmember_jn_dev";
  conn.query(count_sql) do |row|
    count_rows = row[:c];
  end
  conn.close();

  log.d("There are #{count_rows} rows in holdings_htitem_htmember_jn_dev");

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

  # Do a slice at a time until all the rows are exported.
  # One million records per slice.
  slice_size = 1000000;
  slice_seen = 0;

  while slice_seen < count_rows do
    # Generate a mysqldump command in dev 
    # that pipes into a mysql command in prod.

    command = %W[
        mysqldump
        -h #{htrep_dev_host}
        -u #{htrep_dev_user}
        -p#{htrep_dev_pw}
        #{htrep_dev_name} holdings_htitem_htmember_jn_dev
        -w"1 LIMIT #{slice_seen}, #{slice_size}"
        --skip-add-drop-table
        --skip-disable-keys
        --skip-add-locks
        --skip-lock-tables
        --skip-comments
        --skip-set-charset
        --no-create-info
        |
        mysql
        -h #{htrep_prod_host}
        -u #{htrep_prod_user}
        -p#{htrep_prod_pw}
        #{htrep_prod_name}
        ].join(' ');

    ta = Time.new();

    log.d(command);
    system(command);

    # Sleep a portion of time proportionate to the time it 
    # took to export the last slice of records.
    # However, sleep no more than 10 minutes, 
    # and no less than 10 seconds.
    tb = Time.new();
    sleeptime = (tb - ta);
    if sleeptime > 600 then
      sleeptime = 600;
    elsif sleeptime < 10
      sleeptime = 10;
    end
    log.d("Timechange = #{sleeptime}");
    sleep(sleeptime);

    # Count up.
    slice_seen += slice_size;
  end
  check_table(db, log);
end

if $0 == __FILE__ then
  if !Hathienv::Env.is_prod? then
    raise "This script should only be run in prod.";
  end

  log = Hathilog::Log.new();
  log.d("Started");
  db = Hathidb::Db.new();
  truncate_table(db, log);
  export_data_files(db, log);
  log.d("Finished");
end
