require 'phdb/phdb_utils';

=begin

This script is for loading an entire directory of HT003 files,
which are assumed to be the entirety of the known print holdings,
into holdings_memberitem, which is required to be empty in this 
case.

You can also tell it to load a single file. Then holdings_memberitem
does not need to be empty.

=end

# Some useful constants.
HT_DIR = '/htapps/pulintz.babel/data/phdb/HT003/';
HT_RE  = Regexp.new('^HT003_.+\.(mono|multi|serial)\.tsv$');
STOP_F = HT_DIR + '.stop';
DB_SCH = 'ht_repository';
DB_TAB = 'holdings_memberitem';
ZZZ    = 1; # Amount of sleep between loads. Do not set to 0.

# Goes through HT_DIR and looks for files matching HT_RE. 
# Mathcing files are loaded with load_ht_file().
# Stops if told by stop().
def start ()
  t0 = Time.new();
  if !check_holdings_memberitem() then
    puts "#{DB_TAB} must be manually truncated first.";
    return;
  end

  Dir.chdir(HT_DIR);
  ht_files  = [];
  tot_files = 0;
  i         = 0;
  bytes     = 0;

  # Read HT_DIR and save all files matching HT_RE in ht_files[].
  Dir.foreach(HT_DIR) do |file|
    if file =~ HT_RE then
      ht_files << file;
      tot_files += 1;
    end
  end

  # Loop over ht_files[] and process.
  ht_files.each do |file|
    # Always check if we should stop.
    if should_stop then
      puts "Breaking.";
      break;
    end

    full_path = HT_DIR + file;
    i += 1;
    file_size = File.size(full_path).to_i;
    bytes += file_size;

    puts "(#{i}/#{tot_files}) #{file} (#{'%.2f' % (file_size / 2**20)} MB)";
    load_ht_file(full_path);
    t1 = Time.new();
    td = (t1 - t0) - ZZZ;
    puts("This script has been running for #{'%.2f' % td} seconds and loaded #{'%i' % (bytes / 2**20)} MB at #{'%i' % (bytes / td)} bytes/s.");
  end
end

# Performs a LOAD DATA LOCAL INFILE query on a file. 
# Takes a short nap after.
def load_ht_file (path)
  begin
    conn = PHDBUtils.get_dev_conn();
    q = [
         "LOAD DATA LOCAL INFILE '#{path}'",
         "INTO TABLE #{DB_SCH}.#{DB_TAB} IGNORE 1 LINES",
         "(oclc, local_id, member_id, status, item_condition,",
         "process_date, enum_chron, item_type, issn, n_enum, n_chron)"
        ].join(' ');
    puts q;
    conn.update(q);
  rescue StandardError => e
    puts "StandardError when running #{q} ... #{e}";
  ensure
    conn.close();
  end
  if ZZZ > 0 then
    sleep ZZZ;
  end
end

# Called at the beginning of start() to make sure the table is empty.
def check_holdings_memberitem ()
  conn = PHDBUtils.get_dev_conn();
  q = "SELECT COUNT(*) AS rc FROM #{DB_SCH}.#{DB_TAB}";
  ret = false;
  conn.query(q) do |res|
    puts res;
    puts "There are #{res[:rc]} rows in #{DB_TAB}";
    if res[:rc].to_i == 0 then
      ret = true;
    end
  end

  return ret;
end

# Between loads, check if there has been a stop command.
def should_stop ()
  if File.exist?(STOP_F) then
    puts "There is a .stop-file.";
    delete_stop();
    return true;
  else
    return false;
  end
end

# Get rid of the stop file.
def delete_stop ()
  if File.exist?(STOP_F) then
    File.delete(STOP_F);
  end
end

# Make a stop file.
def stop ()
  stop_f = File.open(STOP_F, 'w');
  stop_f.puts(Time.new());
  stop_f.close();
end

# Takes a single arg, 'start' or 'stop'.
# If 'start', then read infiles and load into DB until done or 'stop'ped.
# If 'stop', then make a stop file and do nothing more.

if __FILE__ == $0 then
  usage = "\nUsage:\n\truby #{$0} (<start|stop> | <single> <FILE>)\n\n";

  # Hi-jack Ctrl-c so we can abort gracefully.
  trap("SIGINT") { 
    puts "Caught INTERRUPT signal, will stop as soon as the current LOAD is done...";
    puts "Please be patient, or you risk ruining the mySQL replication.";
    puts "If you HAVE to, you can always:\n\tkill -9 #{Process.pid}";
    stop();
  }

  # Leave nothing behind.
  at_exit {
    delete_stop();
  }

  delete_stop();
  if ARGV.length > 0
    cmd = ARGV.shift;
    if cmd == 'start' then
      start(); 
    elsif cmd == 'stop' then
      stop();
    elsif cmd == 'single' then
      file = ARGV.shift;
      if file == nil then
        puts usage;
      else
        load_ht_file(file);
      end
    else
      puts usage;
    end
  else
    puts usage;
  end
end
