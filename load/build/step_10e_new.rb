require 'hathidb';
require 'hathidata';
require 'hathilog';

db = Hathidb::Db.new();
conn = db.get_conn();

=begin

Get all volume_ids in holdings_htitem_htmember_jn
that aren't held by their collection-mapped member_id.

These are deposited items that haven't yet been reflected
in the member's print holdings data.

Bulk-insert records into holdings_htitem_htmember_jn with
proper member_id a copy count of 1.

The old script assigns a member_id based on source (always member_id=berkeley if source=UC) if no-one holds it.
The new script assigns a member_id based on collection_code, if collection_code->member_id does not hold it.

=end

get_all_sql = %w<
  SELECT hh.volume_id, hcm.member_id, COUNT(DISTINCT h3j.member_id) AS c
  FROM holdings_htitem AS hh
  JOIN hathi_collection_map AS hcm 
  ON (hh.collection_code = hcm.collection)
  LEFT JOIN holdings_htitem_htmember_jn AS h3j 
  ON (hh.volume_id = h3j.volume_id AND hcm.member_id = h3j.member_id)
  WHERE h3j.member_id != ''
  GROUP BY hh.volume_id, hcm.member_id
  HAVING c = 0
>.join(" ")

get_all_q = conn.prepare(get_all_sql);
out_path  = "builds/current/deposits_sans_holdings.tsv";
hdout     = Hathidata::Data.new(out_path).open('w');
log       = Hathilog::Log.new();
log.d(get_all_sql);

i = 0;
get_all_q.enumerate() do |row|
  i += 1;
  if i % 10000 == 0 then
    log.i(i);
  end
  volume_id = row[:volume_id];
  member_id = row[:member_id];
  # Put rows in file, bulk insert later.
  hdout.file.puts [volume_id, member_id, 1].join("\t");
end
log.i(i);

# Load file.
load_sql = %W<
  LOAD DATA LOCAL INFILE ? IGNORE
  INTO TABLE holdings_htitem_htmember_jn
  (volume_id, member_id, copy_count)
>.join(' ');

log.d(load_sql);
load_q = conn.prepare(load_sql);
puts "Not actually inserting!!!";
# load_q.execute(hdout.path.to_s);

hdout.close();
