require 'hathidata';
require 'hathidb';
require 'hathilog';

log = Hathilog::Log.new({:log_sync=>true});

test   = false;
just_h = false;
if ARGV.include?('test') then
  test = true;
end

if ARGV.include?('just_h') then
  just_h = true;
end

shared_print_members = [];
if test == true then
  shared_print_members = %w[cornell duke emory utexas];
else
  Hathidata.read('shared_print_members.tsv') do |line|
    member_id = line.strip;
    shared_print_members << member_id;
  end
end

db   = Hathidb::Db.new();
conn = db.get_conn();

# Special treatment for those who are no longer print holdings members, but still shared print members.
trunc_sql  = "DELETE FROM shared_print_pool WHERE member_id NOT IN ('gatech', 'nrlf', 'srlf')";

# in the old way, oclc_x is the resolved, oclc_y is the variant

insert_sql = %w{
    INSERT INTO shared_print_pool (holdings_memberitem_id, member_id, item_condition, gov_doc, resolved_oclc, local_oclc)
    SELECT hm.id, hm.member_id, hm.item_condition, hm.gov_doc, COALESCE(o.resolved, hm.oclc) AS oclc, hm.oclc AS local_oclc
    FROM holdings_memberitem AS hm
    LEFT JOIN oclc_concordance AS o ON (hm.oclc = o.variant)
    JOIN holdings_cluster_oclc AS hco ON (hco.oclc = COALESCE(o.resolved, hm.oclc))
    WHERE hm.item_type = 'mono'
    AND hm.status NOT IN ('LM', 'WD')
    AND hm.member_id = ?
}.join(' ');

# Special cases go here
delete_special_sql = "DELETE FROM shared_print_pool WHERE id = ?";

utexas_special_sql = %w[
  SELECT spp.id 
  FROM shared_print_pool   AS spp 
  JOIN holdings_memberitem AS hm ON (spp.holdings_memberitem_id = hm.id) 
  WHERE spp.member_id = 'utexas' AND hm.local_id NOT LIKE 'ut.b%'
].join(' ');

get_local_h_sql = %w{
    SELECT resolved_oclc, COUNT(DISTINCT member_id) AS h
    FROM shared_print_pool
    GROUP BY resolved_oclc
    ORDER BY resolved_oclc
}.join(' ');

update_local_h_sql = 'UPDATE shared_print_pool SET local_h = ? WHERE resolved_oclc = ?';

if !just_h then
  # Empty pool.
  log.d(trunc_sql);
  conn.execute(trunc_sql);
  
  # Populate pool.
  insert_q = conn.prepare(insert_sql);
  shared_print_members.each do |member_id|
    log.d(insert_sql.sub('?', "'#{member_id}'"));
    insert_q.execute(member_id);
  end

  # Do special cases before calculating Hs.
  delete_special_q = conn.prepare(delete_special_sql);
  utexas_special_q = conn.prepare(utexas_special_sql);
  log.d("Narrowing down utexas.");
  utexas_special_q.enumerate() do |row|
    delete_special_q.execute(row[:id]);
  end
end


# Calculate local_h (H relative to pool)
get_local_h_q    = conn.prepare(get_local_h_sql);
update_local_h_q = conn.prepare(update_local_h_sql);

log.d("Updating local_h");
i = 0;
get_local_h_q.enumerate() do |row|
  i += 1;
  if i % 10000 == 0 then
    log.d(i);
  end
  oclc = row[:resolved_oclc];
  h    = row[:h];
  update_local_h_q.execute(h, oclc);
end

log.d("Done");
