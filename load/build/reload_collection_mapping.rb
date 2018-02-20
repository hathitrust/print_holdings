require 'hathidb';
require 'hathilog';

log = Hathilog::Log.new();

# Copy collection->member_id mapping from ht_collections in prod.
# Has to be run on a prod machine.

db = Hathidb::Db.new();
dc = db.get_conn();
pc = db.get_prod_conn();

del_sql = "TRUNCATE TABLE hathi_collection_map";
get_sql = "SELECT collection, billing_entity FROM ht_collections ORDER BY collection";
ins_sql = "INSERT INTO hathi_collection_map (collection, member_id) VALUES (?,?)";

del_q = dc.prepare(del_sql); 
get_q = pc.prepare(get_sql);
ins_q = dc.prepare(ins_sql);

# Clean out dev.
del_q.execute();

# Get each record from prod and insert in dev.
get_q.enumerate() do |row|
  collection = row[:collection];
  member_id  = row[:billing_entity];
  log.d("#{collection} => #{member_id}");
  ins_q.execute(collection, member_id);
end
