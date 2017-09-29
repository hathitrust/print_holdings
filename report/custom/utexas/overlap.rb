=begin

+--------------+----------+------+-----------+
| local_id     | oclc     | H    | vol_count |
+--------------+----------+------+-----------+
| ut.b10000446 | 11933056 |   61 |         2 |
| ut.b10000458 | 11483011 |   39 |         2 |
| ut.b10000458 | 11483011 |   40 |         1 |
| ut.b1000046x | 11483211 |   37 |         1 |
| ut.b1000046x | 11483211 |   38 |         1 |
| ut.b10000471 | 11177488 |   64 |         3 |
| ut.b10000483 | 18417935 |   55 |         1 |
| ut.b10000501 |  7906342 |   32 |         1 |
| ut.b10000525 |  8822802 |   47 |         1 |
| ut.b10000550 | 10462853 |   41 |         1 |
+--------------+----------+------+-----------+

=end

require 'hathidb';
require 'hathidata';

db        = Hathidb::Db.new();
conn      = db.get_conn();
member_id = ARGV.shift;

sql = %w{
  SELECT hm.local_id, hco.oclc, h3.H, COUNT(DISTINCT hx.volume_id) AS vol_count
  FROM holdings_cluster_oclc      AS hco
  JOIN holdings_cluster_htitem_jn AS hx ON (hco.cluster_id = hx.cluster_id)
  JOIN holdings_htitem_H AS h3 ON (hx.volume_id = h3.volume_id)
  JOIN holdings_memberitem AS hm on (hco.oclc = hm.oclc)
  WHERE hm.member_id = ?
  GROUP BY hm.local_id, hco.oclc, h3.H
  ORDER BY hm.local_id
}.join(' ');

outfn = "reports/overlap/#{member_id}_$ymd.tsv";
hdout = Hathidata::Data.new(outfn).open('w');

q = conn.prepare(sql);
hdout.file.puts(%w{local_id oclc H vol_count}.join("\t"));
q.enumerate(member_id) do |row|
  hdout.file.puts(row.to_a.join("\t"));
end
