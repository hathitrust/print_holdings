require 'hathidb';
require 'hathiconf';
require 'hathilog';
require 'hathienv';
require 'hathiquery';

# Copied from /htapps/pulintz.babel/Code/phdb/bin/ and slightly modded.

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
  conn = db.get_conn();

  memberids       = [];
  get_members_sql = Hathiquery.get_all_members;
  log.d(get_members_sql);
  conn.query(get_members_sql) do |mrow|
    memberids << mrow[:member_id];
  end

  conf          = Hathiconf::Conf.new();
  htrep_dev_pw  = conf.get('db_pw');
  htrep_prod_pw = conf.get('prod_db_pw');
  count         = 0;

  memberids.each do |mid|
    if mid.length < 2 then
      log.d("Skipping #{mid}, too short.");
      next;
    end
    count += 1;

    command = %W[
        mysqldump
        -h mysql-htdev
        -u ht_repository
        -p#{htrep_dev_pw}
        ht_repository holdings_htitem_htmember_jn_dev
        -w"member_id='#{mid}'"
        --skip-add-drop-table
        --skip-disable-keys
        --skip-add-locks
        --skip-lock-tables
        --no-create-info
        |
        mysql
        -h mysql-sdr
        -u ht_repository
        -p#{htrep_prod_pw}
        ht_repository
        ].join(' ');

    log.d("#{count}: processing #{mid}...");

    ta = Time.new();
    system(command);
    tb = Time.new();

    sleeptime = (tb - ta) / 2;

    if sleeptime > 600 then
      sleeptime = 600;
    end

    log.d("Timechange = #{sleeptime}");
    sleep(sleeptime);
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
