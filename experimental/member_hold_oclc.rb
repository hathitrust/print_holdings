require 'hathidata';
require 'hathidb';

db = Hathidb::Db.new();
conn = db.get_conn();

member_id = ARGV.shift;
oclc_file = ARGV.shift;

sql = %W<
  SELECT DISTINCT h3.h, hco.oclc
  FROM holdings_cluster_oclc AS hco
  JOIN holdings_cluster_htitem_jn AS hchj1
  ON   (hco.cluster_id = hchj1.cluster_id)
  JOIN holdings_cluster_htmember_jn AS hchj2
  ON   (hco.cluster_id = hchj2.cluster_id)
  JOIN holdings_htitem_H AS h3
  ON (h3.volume_id = hchj1.volume_id)
  WHERE
  hchj2.member_id = '#{member_id}'
  AND hco.oclc = ?
>.join(' ');

q = conn.prepare(sql);

hdin      = Hathidata::Data.new(oclc_file).open('r');
hdin.file.each_line do |line|
  oclc = line.strip;
  # puts sql.sub("?", "'#{oclc}'");
  match = false;
  q.enumerate(oclc) do |row|
    puts "#{row[:h]}\t#{row[:oclc]}";
    match = true;
  end
  if !match then
    puts "0\t#{oclc}";
  end

end
