require 'hathidata';
require 'hathidb';
require 'hathilog';

# Jeremy York wrote:
# Sorry, and Martin, could you easily produce a count of how many
# volumes are held by 2 partners total, 3 partners total,
# 4 partners total etc. up to 6, with an indication of how many
# in each category are access = allow?

log = Hathilog::Log.new();
log.d("Started");
conn = Hathidb::Db.new().get_conn();
q    = %W[
  SELECT
      COUNT(access) AS acount,
      hf.rights     AS r,
      hhh.h         AS h
  FROM
      hathi_files                 AS hf,
      holdings_htitem_htmember_jn AS hhhj,
      holdings_htitem_H           AS hhh
  WHERE
      hf.htid = hhhj.volume_id
      AND
      hf.htid = hhh.volume_id
      AND
      hhhj.member_id = 'missouri'
  GROUP BY
      hf.rights,
      hhh.h
  ORDER BY
      h,
      r
].join(' ');

rights = {};
[1, 7, (9 .. 15).to_a, 17].flatten.map{|x| rights[x] = 'allow'};
[2, 5, 8].map{|x| rights[x] = 'deny'};
rights[3] = 'op';

# Dictionary for rights attributes, 'ic' => 2 etc.
rights_name_to_access = {};
conn.query("SELECT name, id FROM ht_rights.attributes") do |row|
  rights_name_to_access[row[:name]] = rights[row[:id]];
  puts "#{row[:name]} => #{row[:id]}";
end

# Group tighter on translated rights attribute.
out_hash = {};
conn.query(q) do |row|
  puts "#{row[:acount]}\t#{row[:r]}\t#{row[:h]}";
  key = "#{rights_name_to_access[row[:r]]}\t#{row[:h]}";
  out_hash[key] ||= 0;
  out_hash[key]  += row[:acount].to_i;
end

# Output, finally.
tot_count    = 0;
broken_count = {};
Hathidata.write('missouri_counts.tsv') do |hdout|
  out_hash.each_key do |k|
    hdout.file.puts "#{out_hash[k]}\t#{k}";
    tot_count += out_hash[k];
  end
  hdout.file.puts "Total count: #{tot_count}";
end
conn.close();
log.d("Finished");
