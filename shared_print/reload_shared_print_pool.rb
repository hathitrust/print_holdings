require 'hathidb';
require 'hathilog';

# shared_print_members = %w[umich wisc yale stanford iastate utexas columbia];
shared_print_members = %w[tufts yale ucdavis];

db   = Hathidb::Db.new();
conn = db.get_conn();

log = Hathilog::Log.new();

trunc_sql  = "TRUNCATE TABLE shared_print_pool";
insert_sql = %w{
    INSERT INTO shared_print_pool (holdings_memberitem_id, member_id, oclc) 
    SELECT hm.id, hm.member_id, COALESCE(o.oclc_x, hm.oclc) AS oclc 
    FROM holdings_memberitem AS hm 
    LEFT JOIN oclc_resolution AS o ON (hm.oclc = o.oclc_y) 
    WHERE hm.item_type = 'mono'
    AND (hm.gov_doc IS NULL OR hm.gov_doc = '0')
    AND hm.member_id = ?
}.join(' ');

log.d(trunc_sql);
conn.execute(trunc_sql);

insert_q = conn.prepare(insert_sql);
shared_print_members.each do |member_id|
  log.d(insert_sql.sub('?', "'#{member_id}'"));
  insert_q.execute(member_id);
end

log.d("Done");
