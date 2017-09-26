require 'hathidata';
require 'hathidb';
require 'hathilog';

db   = Hathidb::Db.new();
conn = db.get_conn();
log  = Hathilog::Log.new();

get_all_volids_sql         = "SELECT volume_id, source, rights, oclcs FROM holdings_htitem ORDER BY volume_id";
get_all_volids_q           = conn.prepare(get_all_volids_sql);
get_members_from_volid_sql = "SELECT DISTINCT member_id FROM holdings_htitem_htmember_jn WHERE volume_id = ?";
get_members_from_volid_q   = conn.prepare(get_members_from_volid_sql);

hdout = Hathidata::Data.new('before_10e_$ymd.tsv').open('w');

i = 0;
get_all_volids_q.enumerate do |v_row|
  volume_id  = v_row[:volume_id];
  source     = v_row[:source];
  rights     = v_row[:rights];
  oclc       = v_row[:oclcs];
  member_ids = [];

  i += 1;
  log.d(i) if i % 100000 == 0;
  
  get_members_from_volid_q.enumerate(volume_id) do |m_row|
    member_ids << m_row[:member_id];
  end

  if member_ids.size == 0 || (member_ids.size == 1 && member_ids.first == '') then
    hdout.file.puts([volume_id,source,rights,oclc].join("\t"));
  end  
end

hdout.close();
