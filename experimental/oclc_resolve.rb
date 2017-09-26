require 'hathidb';

db = Hathidb::Db.new();
conn = db.get_conn();

map_sql = %W[
  SELECT z.a, MIN(z.b) AS min_b FROM (
    SELECT DISTINCT CAST(x.oclc AS UNSIGNED) AS a, CAST(y.oclc AS UNSIGNED) AS b
    FROM holdings_htitem_oclc AS x
    JOIN holdings_htitem_oclc AS y ON (x.volume_id = y.volume_id)
    WHERE x.oclc IN(#{ARGV.join(',')})
  ) AS z GROUP BY a
].join(" ");

puts map_sql;

conn.query(map_sql) do |row|
  puts row.to_a.join("\t");
end
