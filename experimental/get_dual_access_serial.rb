# Find, in a list of volume ids, serials that have both access=allow and access=deny issues.

require 'hathidata';
require 'hathidb';

volids = [];
Hathidata.read(ARGV.shift) do |line|
  line.strip!
  volids << line;
end

db = Hathidb::Db.new();
conn = db.get_conn();
max_slice = 1000;

qmarks = (['?'] * max_slice).join(',');

sql = "SELECT DISTINCT oclcs, access FROM holdings_htitem WHERE item_type = 'serial' AND volume_id IN (#{qmarks})";
q   = conn.prepare(sql);

oclc_access = {};

volids.each_slice(max_slice) do |slice|
  volids = volids - slice;
  while slice.size < max_slice do
    slice << nil;
  end
  puts "#{slice.size}: #{slice.first} .. #{slice.last}";
  q.enumerate(*slice) do |row|
    o = row[:oclcs];
    a = row[:access];
    puts "#{o} #{a}";

    oclc_access[o]    ||= {};
    oclc_access[o][a] = 1;
  end
end

puts "Distinct OCLCS: #{oclc_access.keys.size}";

oclc_access.keys.each do |o|
  as = oclc_access[o].keys;
  put o;
  as.each.sort do |a|
    put "\t#{a}";
  end
end
