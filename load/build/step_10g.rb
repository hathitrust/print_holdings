require 'hathilog';
require 'hathidb';

log  = Hathilog::Log.new();
log.i("Started");

db   = Hathidb::Db.new();
conn = db.get_conn();

q1 = %W<
  SELECT
    member_id,
    COUNT(*) AS c
  FROM
    holdings_htitem_htmember_jn
  WHERE
    lm_count IS NULL
  GROUP BY
    member_id
>.join(' ');

to_log = [];
conn.query(q1) do |row|
  to_log << "#{row[:member_id]}\t#{row[:c]}";
end

logf = Hathilog::Log.new({:file_name => 'build_step_10g.log'});
logf.i("\n" + to_log.join("\n"));
logf.close();
conn.close();
log.i("Finished");
