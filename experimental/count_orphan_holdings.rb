require 'hathidb';

# Get the ones with no member_id.
# Also list their access: allow = pd, deny = ic.

db = Hathidb::Db.new();
conn = db.get_conn();
prefixes = {};
q = [
     "SELECT hhhj.volume_id, hf.access",
     "FROM holdings_htitem_htmember_jn AS hhhj",
     "LEFT JOIN hathi_files AS hf ON (hf.htid = hhhj.volume_id)",
     "WHERE hhhj.member_id = ''"
    ].join(' ');

conn.query(q) do |row|
  pre,post = *row[:volume_id].split('.');
  access   = row[:access];

  prefixes[pre] ||= {};
  prefixes[pre]['count'] ||= 0;
  prefixes[pre]['count']  += 1;
  prefixes[pre][access]  ||= 0;
  prefixes[pre][access]   += 1;
end

# Print sorted by ascending count.
total = 0;
puts %w(member count allow deny).join("\t");
prefixes.keys.sort{|a,b| prefixes[a]['count'] <=> prefixes[b]['count']}.each do |k|
  puts [k, %w(count allow deny).map{|x| prefixes[k][x]}].join("\t");
  total += prefixes[k]['count'];
end
puts "--------";
puts "total\t#{total}";
