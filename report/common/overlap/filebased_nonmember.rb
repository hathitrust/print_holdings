require 'hathidata';
require 'hathidb';

# Assuming name of a file in /data/ with one oclc per line as input.
# We want to know all the overlapping items, but do not care about
# which member they belong to.

oclcs   = [];
overlap = {};
infile  = ARGV.shift;

Hathidata.read(infile) do |line|
  oclcs << line.strip.to_s;
end

db             = Hathidb::Db.new();
conn           = db.get_conn();
hundred_qmarks = (['?'] * 100).join(',');

sql = %W<
SELECT DISTINCT
  hh.access,
  hho.oclc
FROM
  holdings_htitem_oclc AS hho
JOIN
  holdings_htitem AS hh ON (hho.volume_id = hh.volume_id)
WHERE
  hho.oclc IN (#{hundred_qmarks})
>.join(' ');

q = conn.prepare(sql);

# Run query with one hundred bind params each time.
# If we're out of oclc numbers, pad with nils.
while oclcs.size > 0 do
  slice = [];
  STDERR.puts "Slice!";
  1.upto(100).each do |i|
    if oclcs.size > 0 then
      slice << oclcs.shift;
    else
      slice << nil;
    end
  end
  STDERR.puts slice.join(',');

  q.enumerate(*slice) do |row|
    o = row[:oclc].to_s;
    a = row[:access];
    overlap[o] = a;
  end
end

# Only true positives in the list.
overlap.keys.each do |oclc|
  puts [oclc, overlap[oclc]].join("\t");
end
