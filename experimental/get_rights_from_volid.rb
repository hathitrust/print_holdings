require 'hathidata';
require 'hathidb';
require 'hathilog';

db   = Hathidb::Db.new();
conn = db.get_conn();
log  = Hathilog::Log.new();


get_rights_sql = "SELECT rights,oclcs FROM holdings_htitem WHERE volume_id = ?";
get_rights_q   = conn.prepare(get_rights_sql);

Hathidata.read('before_10e_20170509.tsv') do |line|
  line.strip!
  (volume_id,source) = line.split("\t");
  rights = 'BAAAD';
  oclc   = 'BAAAD';
  get_rights_q.enumerate(volume_id) do |row|
    rights = row[:rights];
    oclc   = row[:oclcs];
  end
  puts [volume_id,source,rights,oclc].join("\t");
end

