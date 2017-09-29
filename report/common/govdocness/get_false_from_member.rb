require 'hathidb';

member_id = ARGV.shift;
falseness = ARGV.shift;

if falseness !~ /^false_(pos|neg)/ then
  raise "2nd arg must be false_pos or false_neg";
end

db = Hathidb::Db.new();
conn = db.get_conn();

limit = 0.65;

if falseness == 'false_pos' then
  h_gov_doc = '1';
  calc_percent = "(g.nea / (g.nea + g.yea))";
else
  h_gov_doc = '0';
  calc_percent = "(g.yea / (g.nea + g.yea))";
end

sql = %W[
    SELECT DISTINCT g.oclc, g.yea, g.nea, #{calc_percent} AS p
    FROM govdocness AS g 
    JOIN holdings_memberitem AS h 
    ON (g.oclc = h.oclc) 
    WHERE h.member_id = ?
    AND #{calc_percent} > #{limit}
    AND h.gov_doc = ?
    ORDER BY g.oclc
].join(' ')

query = conn.prepare(sql);

puts sql.sub('?', "'#{member_id}'").sub('?', "'#{h_gov_doc}'");

recs = 0;
query.enumerate(member_id, h_gov_doc) do |row|
  puts "#{row[:oclc]}\tyea:#{row[:yea]}\tnea:#{row[:nea]}\tp:#{row[:p].to_f.round(2)}";

  recs += 1;
end
puts "total #{recs}";
