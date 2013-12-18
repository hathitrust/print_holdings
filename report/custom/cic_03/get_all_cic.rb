require 'hathidb';

db = Hathidb::Db.new();
conn = db.get_conn();
sql  = %q{
SELECT
    h3.volume_id,
    h3.member_id,
    SUM(h3.copy_count) AS cc,
    SUM(h3.access_count) AS ac
FROM
    holdings_htitem AS h2 
    JOIN 
    holdings_htitem_htmember_jn AS h3 ON (h2.volume_id = h3.volume_id)
WHERE
    h3.member_id = ?
    AND
    (h2.item_type = 'mono' OR h2.item_type = 'multi')
GROUP BY
    h3.volume_id,
    h3.member_id;
};

q1 = conn.prepare(sql);
File.open('step_01_out.tsv', 'w') do |outf|
  outf.sync = true;
  members = %w[chi ind iowa minn msu nwu osu psu purd uiuc umd unl uom wisc];
  members.each do |m|
    q1.query(m) do |row|
      outf.puts [
                 row[:volume_id],
                 row[:member_id],
                 row[:cc],
                 row[:ac],
                ].join("\t");
    end 
  end
end
