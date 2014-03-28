require 'hathidb';

# Get the ones with no member_id.

db = Hathidb::Db.new();
conn = db.get_conn();
prefixes = {};
q = "SELECT volume_id FROM holdings_htitem_htmember_jn WHERE member_id = ''";
conn.query(q) do |row|
  pre,post = *row[:volume_id].split('.');
  prefixes[pre] ||= 0;
  prefixes[pre] += 1;
end

prefixes.keys.sort{|a,b| prefixes[a] <=> prefixes[b]}.each do |k|
  puts "#{k}\t#{prefixes[k]}";
end
puts "--------";
puts "total\t#{prefixes.values.reduce(:+)}";
