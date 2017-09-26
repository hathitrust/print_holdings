require 'hathilog';

# Open 2 files. One using log_sync=true, the other false.
# For each, print one line per second for 10 seconds.
# tail -f the 2 files and notice the difference. 

synced_log = Hathilog::Log.new(
  {
    :file_name => 'synced_$ymd.log',
    :log_sync  => true,
  }
);
1.upto(10).each do |i|
  sleep 1;
  synced_log.d(i);
end

unsynced_log = Hathilog::Log.new(
  {
    :file_name => 'unsynced_$ymd.log',
    :log_sync  => false,
  }
);
1.upto(10).each do |i|
  sleep 1;
  unsynced_log.d(i);
end
