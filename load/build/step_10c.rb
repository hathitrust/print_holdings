require 'hathilog';
require 'hathidb';
require 'hathiquery';

# Reinterpretation of:
# /htapps/pete.babel/Code/phdb/lib/sql/create_htitem_htmember_jn_v1.4_nonmulti_stored_procedure.sql
# Todo: this could be multi-threaded? Most of the work is happening in the db so maybe not.
# Todo: rewrite main_sql to use JOIN

log = Hathilog::Log.new();
log.d("Started");

db   = Hathidb::Db.new();
conn = db.get_conn();

count_sql   = "SELECT COUNT(*) AS c FROM holdings_htitem_htmember_jn";
count_query = conn.prepare(count_sql);

count_query.enumerate() do |row|
  log.d("#{count_sql} -- before: #{row[:c]}");
end

main_sql = %w<
    INSERT IGNORE INTO holdings_htitem_htmember_jn 
    (volume_id, member_id, copy_count, lm_count, wd_count, brt_count, access_count)
    SELECT 
        ho.volume_id, 
        mc.member_id, 
        mc.copy_count, 
        mc.lm_count, 
        mc.wd_count, 
        mc.brt_count, 
        mc.access_count
    FROM 
        holdings_htitem_oclc       AS ho, 
        holdings_memberitem_counts AS mc, 
        holdings_htitem            AS h
    WHERE 
        ho.oclc = mc.oclc 
        AND 
        h.volume_id  = ho.volume_id 
        AND 
        h.item_type != 'multi'
        AND 
        mc.member_id = ?;
>.join(' ');
main_query = conn.prepare(main_sql);

members_sql = Hathiquery.get_active_members;

log.d("/*For each member_id in */ #{members_sql}");
log.d("/* ... do: */ #{main_sql}");

conn.query(members_sql) do |row|
  member_id = row[:member_id];
  log.d("Running #{member_id}");
  main_query.execute(member_id);
  count_query.enumerate() do |row|
    log.d("After #{member_id}: #{row[:c]}");
  end
end

conn.close();

log.d("Finished");
