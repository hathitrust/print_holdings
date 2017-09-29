# Want to compare UCLA<->SRLF and Berkeley<->NRLF.

require 'hathidb';

db = Hathidb::Db.new();
conn = db.get_conn();

lib_pairs = [['ucla', 'srlf'], ['berkeley', 'nrlf']];

sql = "SELECT DISTINCT volume_id FROM holdings_htitem_htmember_jn WHERE member_id = ?";
q   = conn.prepare(sql);

lib_pairs.each do |pair|
  puts "comparing #{pair[0]} and #{pair[1]}";
  holdings = {};
  [0,1].each do |i|
    puts "querying #{pair[i]}...";
    q.enumerate(pair[i]) do |row|
      v = row[:volume_id];
      holdings[v] ||= [];
      holdings[v] << i;
    end
  end

  counts = {
    "only #{pair[0]}" => 0,
    "only #{pair[1]}" => 0,
    "overlap"         => 0,
  };
  
  holdings.keys.each do |k|
    case holdings[k]
    when [0] then counts["only #{pair[0]}"] += 1;
    when [1] then counts["only #{pair[1]}"] += 1;
    when [0,1] then counts["overlap"] += 1;
    else puts "huh"
    end
  end

  counts.each do |label, count|
    puts [label, count].join(": ");
  end
end
