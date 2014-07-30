require 'hathilog';
require 'hathidb';
require 'hathiquery';

log  = Hathilog::Log.new();
log.d("Started");
db   = Hathidb::Db.new();
conn = db.get_conn();

get_member_sql = Hathiquery.get_active_members;

insert_sql = %W<
  INSERT INTO 
    holdings_H_counts (H_id, member_id, access, item_type, H_count)
  SELECT 
    hh.H, hhj.member_id, h.access, h.item_type, COUNT(DISTINCT hh.volume_id)
  FROM 
    holdings_htitem AS h, holdings_htitem_H AS hh, holdings_htitem_htmember_jn AS hhj
  WHERE 
    hh.volume_id = hhj.volume_id AND hh.volume_id = h.volume_id AND hhj.member_id = ?
  GROUP BY 
    hh.H, h.access, h.item_type
>.join(" ");

insert_query = conn.prepare(insert_sql);
count_sql    = "SELECT COUNT(*) AS c FROM holdings_H_counts WHERE member_id = ?";
count_query  = conn.prepare(count_sql);

truncate_sql = "TRUNCATE holdings_H_counts";
log.d(truncate_sql);
conn.update(truncate_sql);

log.d(get_member_sql)
conn.query(get_member_sql) do |gmrow|
  member_id = gmrow[:member_id];
  log.d(member_id + ' ...');
  insert_query.execute(member_id);
  count_query.enumerate(member_id) do |cqrow|
    log.d(cqrow[:c]);
  end
  sleep(1);
end

conn.close();
log.d("Finished");
