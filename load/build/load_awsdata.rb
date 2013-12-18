require 'hathilog';
require 'hathidb';
require 'hathidata';

# A quick and dirty slap-together of pieces from load_HT003.rb.
# Takes a directory as input and loads files matching a pattern
# into a database table.

DB_SCH = 'ht_repository';
DB_TAB = 'holdings_memberitem_counts';
AWS_RE = Regexp.new('^part-r-[0-9]+$');
ZZZ    = 1;

$db  = Hathidb::Db.new();
$log = Hathilog::Log.new();

def start (dir)
  if !check_table_empty then
    $log.e("Table #{DB_TAB} should be empty first. Will not proceed unless it is truncated.");
    return;
  end

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
    load_aws_file(full_path);
  end
end

# Called at the beginning of start() to make sure the table is empty.
def check_table_empty ()
  conn = $db.get_conn();
  q = "SELECT COUNT(*) AS rc FROM #{DB_SCH}.#{DB_TAB}";
  ret = false;
  conn.query(q) do |res|
    $log.d("There are #{res[:rc]} rows in #{DB_TAB}");
    if res[:rc].to_i == 0 then
      ret = true;
    end
  end
  conn.close();

  return ret;
end

def load_aws_file (path)
  $log.i("Started #{path}");
  begin
    conn = $db.get_conn();
    q = "LOAD DATA LOCAL INFILE '#{path}' INTO TABLE #{DB_SCH}.#{DB_TAB}";

    $log.d(q);
    conn.update(q);
  rescue StandardError => e
    $log.e("StandardError when running #{q} ... #{e}");
  ensure
    conn.close();
  end
end

if __FILE__ == $0 then
  usage = "\nUsage:\n\truby #{$0} <start> <DIR>\n\n";

  if ARGV.length > 0
    cmd = ARGV.shift;
    if cmd == 'start' then
      dir = ARGV.shift;
      if dir != nil then
        $log.i("Started");
        start(dir);
      end
    else
      puts usage;
    end
  else
    puts usage;
  end
  $log.i("Finished");
end
