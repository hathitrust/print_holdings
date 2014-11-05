require 'hathidb';

# Get the ones with no member_id.
# Also list their access: allow = pd, deny = ic.

db = Hathidb::Db.new();
conn = db.get_conn();
prefixes = {};
q = [
     "SELECT volume_id",
     "FROM holdings_htitem_htmember_jn",
     "WHERE member_id = ''"
    ].join(' ');

conn.query(q) do |row|
  pre,post = *row[:volume_id].split('.');

  prefixes[pre] ||= {};
  prefixes[pre]['count'] ||= 0;
  prefixes[pre]['count']  += 1;
  prefixes[pre]['guess']  = [];
end

# See if a member holds other volumes with the same prefix.

# Print sorted by ascending count.
total = 0;
puts %w(member count).join("\t");
prefixes.keys.sort{|a,b| prefixes[a]['count'] <=> prefixes[b]['count']}.each do |k|
  puts [k, %w(count allow deny).map{|x| prefixes[k][x]}].join("\t");
  total += prefixes[k]['count'];
end
puts "--------";
puts "total\t#{total}";
