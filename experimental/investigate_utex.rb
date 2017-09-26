require 'hathidb';
require 'hathidata';
require 'hathilog';

db = Hathidb::Db.new();
logger = Hathilog::Log.new();

# compare dec and current version of table, with regard to utexas and their H.

# create some tables like some other tables
if ARGV.include?('reload') then
  iconn = db.get_interactive();
  [
    "DROP TABLE IF EXISTS holdings_htitem_htmember_jn_dec_utexas",
    "DROP TABLE IF EXISTS holdings_htitem_htmember_jn_cur_utexas",  
    "DROP TABLE IF EXISTS holdings_htitem_htmember_jn_dec_other",
    "DROP TABLE IF EXISTS holdings_htitem_htmember_jn_cur_other",      
    "CREATE TABLE holdings_htitem_htmember_jn_dec_utexas LIKE holdings_htitem_htmember_jn",
    "INSERT INTO holdings_htitem_htmember_jn_dec_utexas (volume_id, member_id) SELECT t.volume_id, 'utexas' FROM holdings_htitem_htmember_jn_dec AS t WHERE t.member_id = 'utexas'",
    "CREATE TABLE holdings_htitem_htmember_jn_cur_utexas LIKE holdings_htitem_htmember_jn",  
    "INSERT INTO holdings_htitem_htmember_jn_cur_utexas (volume_id, member_id) SELECT t.volume_id, 'utexas' FROM holdings_htitem_htmember_jn AS t WHERE t.member_id = 'utexas'",
    "CREATE TABLE holdings_htitem_htmember_jn_dec_other LIKE holdings_htitem_htmember_jn",
    "INSERT INTO holdings_htitem_htmember_jn_dec_other (volume_id, member_id) SELECT t1.volume_id, t1.member_id FROM holdings_htitem_htmember_jn_dec AS t1 JOIN holdings_htitem_htmember_jn_dec_utexas AS t2 ON (t1.volume_id = t2.volume_id)",
    "CREATE TABLE holdings_htitem_htmember_jn_cur_other LIKE holdings_htitem_htmember_jn",
    "INSERT INTO holdings_htitem_htmember_jn_cur_other (volume_id, member_id) SELECT t1.volume_id, t1.member_id FROM holdings_htitem_htmember_jn AS t1 JOIN holdings_htitem_htmember_jn_cur_utexas AS t2 ON (t1.volume_id = t2.volume_id)",
  ].each do |q|
    logger.d("tick tock");
    puts q;
    iconn.execute(q);    
  end
  iconn.close();
end

# Get all volume_ids
get_ids_count = "SELECT COUNT(DISTINCT x.volume_id) AS c FROM (SELECT t1.volume_id FROM holdings_htitem_htmember_jn_dec_utexas AS t1 UNION SELECT t2.volume_id FROM holdings_htitem_htmember_jn_cur_utexas AS t2) AS x";
get_ids       = "SELECT DISTINCT x.volume_id FROM (SELECT t1.volume_id FROM holdings_htitem_htmember_jn_dec_utexas AS t1 UNION SELECT t2.volume_id FROM holdings_htitem_htmember_jn_cur_utexas AS t2) AS x";
get_h_cur_sql = "SELECT COUNT(DISTINCT member_id) AS c FROM holdings_htitem_htmember_jn_cur_other WHERE volume_id = ?";
get_h_dec_sql = "SELECT COUNT(DISTINCT member_id) AS c FROM holdings_htitem_htmember_jn_dec_other WHERE volume_id = ?";

conn = db.get_conn();
get_h_cur_q = conn.prepare(get_h_cur_sql);
get_h_dec_q = conn.prepare(get_h_dec_sql);

# Get count
count_ids = 0;
logger.d(get_ids_count);
conn.query(get_ids_count) do |row|
  logger.d(row[:c]);
  count_ids = row[:c].to_i;
end

hdout = Hathidata::Data.new("investigate_utexas_$ymd.tsv").open('w');

# Loop volume_ids
i = 0;
conn.query(get_ids) do |row|
  i += 1;
  if i % 5000 == 0 then
    logger.d("#{i} / #{count_ids} (#{i.to_f / count_ids}%)");
  end

  v = row[:volume_id];
  count_dec = 0;
  count_cur = 0;
  get_h_dec_q.enumerate(v) do |row_dec|
    count_dec = row_dec[:c].to_i;
  end
  get_h_cur_q.enumerate(v) do |row_cur|
    count_cur = row_cur[:c].to_i;
  end
  diff = count_cur - count_dec;
  hdout.file.puts([v, count_dec, count_cur, diff].join("\t"));
end

hdout.close();
