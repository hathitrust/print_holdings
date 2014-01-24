require 'hathilog';
require 'hathidb';

# Reinterpretation of:
# /htapps/pulintz.babel/Code/phdb/lib/sql/create_htitem_htmember_jn_v1.4_nonmulti_stored_procedure.sql

log = Hathilog::Log.new();
log.d("Started");

db   = Hathidb::Db.new();
conn = db.get_conn();

main_sql = %W<
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

members_sql = "SELECT DISTINCT member_id FROM holdings_htmember WHERE status = 1";

log.d("/*For each member_id in */ #{members_sql}");
log.d("/* ... do: */ #{main_sql}");

conn.query(members_sql) do |row|
  member_id = row[:member_id];
  log.d("Running #{member_id}");
  main_query.execute(member_id);
end

conn.close();

log.d("Finished");
