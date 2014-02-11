require 'hathidata';
require 'hathidb';
require 'hathilog';

# Jeremy York wrote:
# Sorry, and Martin, could you easily produce a count of how many
# volumes are held by 2 partners total, 3 partners total,
# 4 partners total etc. up to 6, with an indication of how many
# in each category are access = allow?

log = Hathilog::Log.new();
log.d("Started");

db   = Hathidb::Db.new();
conn = db.get_conn();
q    = %W[
  SELECT
      COUNT(access) AS acount,
      hf.access     AS a,
      hhh.h         AS h
  FROM
      hathi_files                 AS hf,
      holdings_htitem_htmember_jn AS hhhj,
      holdings_htitem_H           AS hhh
  WHERE
      hf.htid = hhhj.volume_id
      AND
      hf.htid = hhh.volume_id
      AND
      hhhj.member_id = 'missouri'
  GROUP BY
      hf.access,
      hhh.h
  ORDER BY
      h,
      a
].join(' ');

cols = [:acount, :a, :h];

Hathidata.write('missouri_counts.tsv') do |hdout|
  conn.query(q) do |row|
    hdout.file.puts cols.map{|c| row[c]}.join("\t");
  end
end

conn.close();
log.d("Finished");
