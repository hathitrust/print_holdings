require 'hathidata';
require 'hathidb';

db = Hathidb::Db.new();
conn = db.get_conn();
q = conn.prepare("select distinct oclc from holdings_memberitem where member_id = 'berkeley' and oclc = ?");

in_berkeley = {
  true  => 0,
  false => 0
};

i = 1;
Hathidata.read("nrlf_rejects_check_berkeley.txt") do |line|
  if i % 1000 == 0 then
    puts i;
  end
  oclc = line.strip;
  in_b = false;
  q.query(oclc) do |row|
    in_b = true;
  end
  in_berkeley[in_b] += 1;  
  i += 1;
end

in_berkeley.keys.each do |k|
  puts "in berkeley? #{k} #{in_berkeley[k]}";
end
