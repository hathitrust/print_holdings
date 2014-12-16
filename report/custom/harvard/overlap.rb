require 'hathidata';
require 'hathidb';

# Takes a file of OCLCs as input, outputs a list of overlaps with HT, given as oclc \t item_type \t count.

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
SELECT
  hho.oclc,
  hh.item_type
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
  1.upto(100).each do |i|
    if oclcs.size > 0 then
      slice << oclcs.shift;
    else
      slice << nil;
    end
  end

  q.enumerate(*slice) do |row|
    o = row[:oclc].to_s;
    overlap[o] ||= {};
    i = row[:item_type];
    overlap[o][i] ||= 0;
    overlap[o][i] += 1;
  end
end

# Screenbarf.
puts %w[oclc item_type count].join("\t");
overlap.keys.each do |oclc|
  overlap[oclc].keys.each do |item_type|
    puts [oclc, item_type, overlap[oclc][item_type]].join("\t");
  end
end
