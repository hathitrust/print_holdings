require 'hathidata';
require 'hathidb';

# Take a list of oclcs and count how many match HT.
# No strings attached, no questions asked.

oclcs   = [];
infile  = ARGV.shift;

Hathidata.read(infile) do |line|
  oclcs << line.strip.to_s;
end

db     = Hathidb::Db.new();
conn   = db.get_conn();
qmarks = (['?'] * 1000).join(',');

sql = %W<
SELECT
  COUNT(DISTINCT oclc) AS c
FROM
  holdings_htitem_oclc
WHERE
  oclc IN (#{qmarks})
>.join(' ');

q = conn.prepare(sql);
match_count = 0;
puts "Total oclcs: #{oclcs.size}";
while oclcs.size > 0 do
  slice = [];
  1.upto(1000).each do |i|
    if oclcs.size > 0 then
      slice << oclcs.shift;
    else
      slice << nil;
    end
  end
  q.enumerate(*slice) do |row|
    match_count += row[:c].to_i;
  end
end

puts "Matching oclcs: #{match_count}";
