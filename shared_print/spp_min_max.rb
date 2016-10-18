require 'hathidata';
require 'hathidb';

# Output all oclcs in the shared print pool  that have a local_h <= x and >= y.

db     = Hathidb::Db.new();
conn   = db.get_conn();
hdout  = Hathidata::Data.new("reports/shared_print/min_max_$ymd.tsv").open('w');
min    = 5;
max    = 35;
h_freq = {};

report_sql = %w{
  SELECT DISTINCT oclc, local_h
  FROM shared_print_pool
  WHERE (local_h <= ? OR local_h >= ?)
  ORDER BY local_h, oclc
}.join(' ');

hdout.file.puts("oclc\tH");

report_q = conn.prepare(report_sql);
report_q.enumerate(min, max) do |row|
  h    = row[:local_h].to_i;
  oclc = row[:oclc];
  h_freq[h] ||= 0;
  h_freq[h] += 1
  hdout.file.puts("#{oclc}\t#{h}");
end
hdout.close();

# Put freqs in separate file.
hdout_freq = Hathidata::Data.new("reports/shared_print/min_max_$ymd_freq.tsv").open('w');
hdout_freq.file.puts("H\tfreq");
h_freq.keys.sort.each do |h|
  hdout_freq.file.puts("#{h}\t#{h_freq[h]}");
end
hdout_freq.close();

