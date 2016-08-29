require 'hathilog';
require 'hathidb';

# Part of step 9.
# A quick and dirty slap-together of pieces from load_HT003.rb.
# Takes a directory as input and loads files matching a pattern
# into a database table.

DB_SCH = 'ht_repository';
DB_TAB = 'holdings_memberitem_counts';
AWS_RE = Regexp.new('^part-.+[0-9]+$');
ZZZ    = 1;

def start (dir, db, log)
  ensure_table_empty(db, log);

  Dir.chdir(dir);
  aws_files = [];

  Dir.foreach(dir) do |file|
    if file =~ AWS_RE then
      aws_files << file;
    end
  end

  # Loop over aws_files[] and process.
  aws_files.each do |file|
    full_path = dir + file;
    load_aws_file(full_path, db, log);
  end

  check_counts(db, log);
end

# Called at the beginning of start() to make sure the table is empty.
def ensure_table_empty (db, log)
  has_rows = check_counts(db, log);
  conn = db.get_conn();
  if has_rows then
    q = "TRUNCATE TABLE #{DB_SCH}.#{DB_TAB}";
    log.d(q);
    conn.execute(q);
  end
  conn.close();
end

# Called before and after the reload.
def check_counts(db, log)
  conn = db.get_conn();
  q = "SELECT COUNT(*) AS rc FROM #{DB_SCH}.#{DB_TAB}";
  has_rows = true;
  log.d(q);
  conn.query(q) do |res|
    log.d("There are #{res[:rc]} rows in #{DB_TAB}");
    if res[:rc].to_i == 0 then
      false;
    end
  end
  conn.close();
  return has_rows;
end

def load_aws_file (path, db, log)
  log.i("Started #{path}");
  begin
    conn = db.get_conn();
    q = "LOAD DATA LOCAL INFILE '#{path}' INTO TABLE #{DB_SCH}.#{DB_TAB}";

    log.d(q);
    conn.update(q);
  rescue StandardError => e
    log.e("StandardError when running #{q} ... #{e}");
  ensure
    conn.close();
  end
end

if __FILE__ == $0 then
  usage = "\nUsage:\n\truby #{$0} <start> <DIR>\n\n";
  log = Hathilog::Log.new();
  if ARGV.length > 0 then
    cmd = ARGV.shift;
    if cmd == 'start' then
      db  = Hathidb::Db.new();
      dir = ARGV.shift;
      if dir != nil then
        log.i("Started");
        start(dir, db, log);
      end
    else
      log.e(usage);
    end
  else
    log.e(usage);
  end
  log.i("Finished");
end
