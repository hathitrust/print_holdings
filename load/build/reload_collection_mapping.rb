require 'hathidb';
require 'hathilog';

log = Hathilog::Log.new();

# Copy collection->member_id mapping from ht_collections in prod.
# Has to be run on a prod machine.

db = Hathidb::Db.new();
dc = db.get_conn();
pc = db.get_prod_conn();

# Got to make sure we don't get mappings for members that don't exist in dev.
get_target_member_ids = "SELECT member_id FROM holdings_htmember";
member_ids = [];
dc.query(get_target_member_ids) do |row|
  member_ids << row[:member_id];
end

del_sql = "TRUNCATE TABLE hathi_collection_map";

get_source_sql = %W<
  SELECT collection, billing_entity
  FROM ht_collections
  WHERE billing_entity IN (#{member_ids.map{|x| '?'}.join(',')})
  ORDER BY collection
>.join(' ');
get_target_sql = "SELECT collection, member_id FROM hathi_collection_map ORDER BY collection";
ins_sql = "INSERT INTO hathi_collection_map (collection, member_id) VALUES (?,?)";

get_target_q = dc.prepare(get_target_sql)
del_q = dc.prepare(del_sql);
get_source_q = pc.prepare(get_source_sql);
ins_q = dc.prepare(ins_sql);

# Get current mapping in dev (for diff purposes).
old_map = [];
get_target_q.enumerate() do |row|
  old_map << row.to_a;
  puts "Old: #{row.to_a.join(' => ')}";
end

# Clean out dev.
del_q.execute();

# Get each record from prod and insert in dev.
get_source_q.enumerate(*member_ids) do |row|
  collection = row[:collection];
  member_id  = row[:billing_entity];
  log.d("#{collection} => #{member_id}");
  ins_q.execute(collection, member_id);
end

# Get even currenter mapping in dev (for diff purposes).
new_map = [];
get_target_q.enumerate() do |row|
  new_map << row.to_a;
  puts "New: #{row.to_a.join(' => ')}";
end

diff_added   = new_map - old_map;
diff_removed = old_map - new_map;

# Show diff.
if diff_added.size > 0 then
  puts "Diff (added) :";
  diff_added.each do |d|
    puts d.join(" => ");
  end
end
if diff_removed.size > 0 then
  puts "Diff (removed) :";
  diff_removed.each do |d|
    puts d.join(" => ");
  end
end
