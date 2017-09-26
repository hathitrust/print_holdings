require 'hathidb';
require 'hathienv';

db = Hathidb::Db.new();
conn = db.get_conn();

holdings_table = 'holdings_htitem_htmember_jn';
if Hathienv::Env.is_prod? then
  puts "\n\n\n*** SURE YOU WANT TO DO THIS IN PRODUCTION?? ***\n\n\n";
  holdings_table = 'holdings_htitem_htmember';
  conn = db.get_prod_interactive();
end

# Get latest version.
max_version_sql = "SELECT MAX(version) AS max_v FROM holdings_deltas";
puts max_version_sql;
max_version     = -1;
conn.query(max_version_sql) do |row|
  max_version = row[:max_v];
  puts max_version;
end

# Get the ones that ONLY exist in holdings_table as member_id = '' and is in holdings_deltas.
get_volumeids_sql = %W[
  SELECT h3.volume_id, COUNT(h3.member_id) AS c
  FROM #{holdings_table} AS h3
  JOIN #{holdings_table} AS x ON (x.volume_id = h3.volume_id)
  JOIN holdings_deltas AS hd ON (h3.volume_id = hd.volume_id)
  WHERE hd.version = ? AND x.member_id = ''
  GROUP BY h3.volume_id HAVING c = 1
].join(' ');
puts get_volumeids_sql;
get_volumeids_q = conn.prepare(get_volumeids_sql);

# Put volume ids in an array.
volume_ids = [];
get_volumeids_q.enumerate(max_version) do |row|
  volume_ids << row[:volume_id];
end

volume_ids.each do |v|
  "DELETEing #{v} FROM holdings_deltas";
end

# Deal with holdings_deltas, only the ids in volume_ids
del_deltas_sql = "DELETE FROM holdings_deltas WHERE version = ? AND volume_id IN(#{volume_ids.map{|x| "'#{x}'"}.join(',')})";
puts del_deltas_sql;
del_deltas_q   = conn.prepare(del_deltas_sql);
del_deltas_q.execute(max_version)

# Deal with holdings_table, all member_id=''.
del_2_sql = "DELETE FROM #{holdings_table} WHERE member_id = ''";
puts del_2_sql;
del_2_q   = conn.prepare(del_2_sql);
del_2_q.execute();

# Check:
check_sql_1 = "SELECT COUNT(volume_id) AS c FROM #{holdings_table} WHERE member_id = ''";
puts check_sql_1;
conn.query(check_sql_1) do |row|
  puts row[:c];
end

check_sql_2 = "SELECT COUNT(volume_id) AS c FROM holdings_deltas WHERE version = ? AND volume_id IN (#{volume_ids.map{|x| "'#{x}'"}.join(',')})";
puts check_sql_2;
check_q_2 = conn.prepare(check_sql_2);
check_q_2.enumerate(max_version) do |row|
  puts row[:c];
end
